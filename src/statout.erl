-module(statout).
-behaviour(gen_server).

-export([
          start/1,
          start_link/1,
          stop/0,

          command/0,

          init/1,
          handle_call/3,
          handle_cast/2,
          handle_info/2,
          code_change/3,
          terminate/2
        ]).

-include_lib("kernel/include/file.hrl").

-record(state, {
          cache_file
        }).

%% Exported

start(Options) ->
    gen_server:start({local, ?MODULE}, ?MODULE, Options, []).

start_link(Options) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, Options, []).

stop() ->
    gen_server:call(?MODULE, stop).

command() ->
    gen_server:call(?MODULE, command).


%% Internal

init(_Options) ->
    CacheFileName = filename:join([application:get_env(statist, base_dir, "data"), "cache"]),
    ets:delete(?MODULE),
    State = #state{cache_file = CacheFileName},
    load_cache(State),
    erlang:start_timer(5*60*1000, self(), write_cache),
    {ok, State}.


load_cache(#state{cache_file = CacheFile} = State) ->
    case ets:file2tab(CacheFile, [{verify, true}]) of
        {ok, ?MODULE} -> ok;
        {ok, OtherName} ->
            ets:delete(OtherName),
            ok = init_cache(State),
            load_cache(State);
        {error, _Reason} ->
            ok = init_cache(State),
            load_cache(State)
    end.

init_cache(#state{} = State) ->
    ok = ets:delete(?MODULE),
    ets:new(?MODULE, [named_table,
                      {write_concurrency, false},
                      {read_concurrency, true},
                      compressed]),
    dump_cache(State).

dump_cache(#state{cache_file = CacheFile}) ->
    ets:tab2file(?MODULE, CacheFile, [{extended_info, [object_count]}]).


handle_call({get, _, From, To}, _From, #state{} = State) when From > To ->
    {reply, {error, "incorrect date"}, State};
handle_call({get, MapReduce, From, To}, _From, #state{} = State) ->
    Result = get_from_cache_or_calculate(MapReduce, {From, To}),
    {reply, Result, State};

handle_call(stop, _From, #state{} = State) ->
    {stop, normal, stopped, State};

handle_call(Unexpected, _From, #state{} = State) ->
    {stop, {error_unexpected, Unexpected}, error_unexpected, State}.


handle_cast(command, #state{} = State) ->
    {noreply, State};

handle_cast(Unexpected, #state{} = State) ->
    {stop, {error_unexpected, Unexpected}, State}.


handle_info(write_cache, State) ->
    dump_cache(State),
    {noreply, State};

handle_info(Unexpected, #state{} = State) ->
    {stop, {error_unexpected, Unexpected}, State}.


code_change(_OldVsn, #state{} = State, _Extra) ->
    {ok, State}.


terminate({error_unexpected, _Unexpected}, #state{} = _State) ->
    %% TODO: report about unexpected
    ok;

terminate(_Reason, #state{} = _State) ->
    ok.

get_from_cache_or_calculate(MapReduce, {From, To} = Interval) ->
    case from_cache(MapReduce, Interval) of
        {ok, Result} -> Result;
        _ ->
            Files = statpartition:filepaths_for_interval(From, To),
            Result = map_reduce_and_cache_intermediary(MapReduce, Files),
            to_cache(MapReduce, Interval, Result),
            Result
    end.

map_reduce_and_cache_intermediary(MapReduce, Files) ->
    Results = vutil:pmap(fun(File) ->
                                 case from_cache(MapReduce, {file, File}) of
                                     {ok, Result} -> {cached, Result};
                                     _ -> {not_cached, File, statmr:map(MapReduce, File)}
                                 end
                         end, Files),
    Results1 = lists:map(fun({cached, Result}) -> Result;
                            ({not_cached, File, Result}) ->
                                 to_cache(MapReduce, {file, File}, Result),
                                 Result
                         end, Results),
    statmr:reduce(MapReduce, Results1).


from_cache(MapReduce, {file, File}) ->
    FreshMTime = get_mtime(File),
    case ets:lookup(?MODULE, {MapReduce, File}) of
        [] -> no;
        [{{MapReduce, File}, Value, FreshMTime}] -> {ok, Value};
        [{{MapReduce, File}, _, _}] ->
            ets:delete(MapReduce, {MapReduce, File}),
            no
    end;

from_cache(MapReduce, Item) ->
    case ets:lookup(?MODULE, {MapReduce, Item}) of
        [] -> no;
        [{{MapReduce, Item}, Value}] -> {ok, Value}
    end.


to_cache(MapReduce, {file, File}, Value) ->
    FreshMTime = get_mtime(File),
    ets:insert(?MODULE, {{MapReduce, File}, Value, FreshMTime});
to_cache(MapReduce, Item, Value) ->
    ets:insert(?MODULE, {{MapReduce, Item}, Value}).


get_mtime(File) ->
    case file:read_file_info(File) of
        {ok, #file_info{mtime = MTime}} -> MTime;
        _ -> undefined
    end.
