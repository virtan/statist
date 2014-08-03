-module(statout).
-behaviour(gen_server).

-export([
          start/1,
          start_link/1,
          stop/0,

          get/3,

          init/1,
          handle_call/3,
          handle_cast/2,
          handle_info/2,
          code_change/3,
          terminate/2,

          test_processor/0
        ]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("kernel/include/file.hrl").
-include_lib("statist/include/statprocessor.hrl").

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

get(Processor, From, To) ->
    gen_server:call(?MODULE, {get, Processor, From, To}).


%% Internal

init(_Options) ->
    CacheFileName = filename:join([application:get_env(statist, base_dir, "data"), "cache"]),
    State = #state{cache_file = CacheFileName},
    ok = filelib:ensure_dir(CacheFileName),
    load_cache(State),
    erlang:start_timer(5*60*1000, self(), write_cache),
    {ok, State}.


load_cache(#state{cache_file = CacheFile} = State) ->
    catch ets:delete(?MODULE),
    case ets:file2tab(CacheFile, [{verify, true}]) of
        {ok, ?MODULE} -> ok;
        {ok, OtherName} ->
            catch ets:delete(OtherName),
            ok = init_cache(State),
            load_cache(State);
        {error, _Reason} ->
            ok = init_cache(State),
            load_cache(State)
    end.

init_cache(#state{} = State) ->
    catch ets:delete(?MODULE),
    ets:new(?MODULE, [named_table,
                      {write_concurrency, false},
                      {read_concurrency, true},
                      compressed]),
    dump_cache(State).

dump_cache(#state{cache_file = CacheFile}) ->
    ets:tab2file(?MODULE, CacheFile, [{extended_info, [object_count]}]).


handle_call({get, _, From, To}, _From, #state{} = State) when From > To ->
    {reply, {error, "incorrect date"}, State};
handle_call({get, Processor, From, To}, _From, #state{} = State) ->
    case statprocessor:has_processor(Processor) of
        true ->
            Result = get_from_cache_or_calculate(Processor, {From, To}),
            {reply, Result, State};
        _ ->
            {reply, no_such_processor, State}
    end;

handle_call(stop, _From, #state{} = State) ->
    {stop, normal, stopped, State};

handle_call(Unexpected, _From, #state{} = State) ->
    {stop, {error_unexpected, Unexpected}, error_unexpected, State}.


handle_cast(command, #state{} = State) ->
    {noreply, State};

handle_cast(Unexpected, #state{} = State) ->
    {stop, {error_unexpected, Unexpected}, State}.


handle_info({timeout, _, write_cache}, State) ->
    dump_cache(State),
    erlang:start_timer(5*60*1000, self(), write_cache),
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

get_from_cache_or_calculate(Processor, {From, To}) ->
    Files = statpartition:filepaths_for_interval(From, To),
    process_and_cache_intermediary(Processor, Files).
    %case from_cache(Processor, Interval) of
    %    {ok, Result} -> Result;
    %    _ ->
    %        Files = statpartition:filepaths_for_interval(From, To),
    %        Result = process_and_cache_intermediary(Processor, Files),
    %        to_cache(Processor, Interval, Result),
    %        Result
    %end.

process_and_cache_intermediary(Processor, Files) ->
    ProcessorObj = statprocessor:get_processor(Processor),
    Results = vutil:pmap(fun(File) ->
              case from_cache(statprocessor:low_key(ProcessorObj), {file, File}) of
                  {ok, Result} -> {cached, Result};
                  _ -> {not_cached, File, statprocessor:low(ProcessorObj, File)}
              end
         end, Files),
    Results1 = lists:map(fun({cached, Result}) -> Result;
              ({not_cached, File, Result}) ->
                    to_cache(statprocessor:low_key(ProcessorObj), {file, File}, Result),
                    Result
         end, Results),
    statprocessor:high(ProcessorObj, Results1).


from_cache(Processor, {file, File}) ->
    FreshMTime = get_mtime(File),
    case ets:lookup(?MODULE, {Processor, File}) of
        [] -> no;
        [{{Processor, File}, Value, FreshMTime}] -> {ok, Value};
        [{{Processor, File}, _, _}] ->
            ets:delete(?MODULE, {Processor, File}),
            no
    end;

from_cache(Processor, Item) ->
    case ets:lookup(?MODULE, {Processor, Item}) of
        [] -> no;
        [{{Processor, Item}, Value}] -> {ok, Value}
    end.


to_cache(Processor, {file, File}, Value) ->
    FreshMTime = get_mtime(File),
    ets:insert(?MODULE, {{Processor, File}, Value, FreshMTime});
to_cache(Processor, Item, Value) ->
    ets:insert(?MODULE, {{Processor, Item}, Value}).


get_mtime(File) ->
    case file:read_file_info(File) of
        {ok, #file_info{mtime = MTime}} -> MTime;
        _ -> undefined
    end.


%% Tests

global_test() ->
    os:cmd("rm -rf __data_temp"),
    %file:make_dir("__data_temp"),
    application:set_env(statist, base_dir, "__data_temp"),
    application:start(statist),
    gen_server:cast(statist, {event, {1350, 762937, 1213}, [{hello, world}]}),
    gen_server:cast(statist, {event, {1402, 12, 13}, [{hello, world}]}),
    gen_server:cast(statist, {event, {1407, 1000, 1212}, [{hello, world}]}),
    timer:sleep(300),
    statprocessor:processor_module(?MODULE),
    Res1 = statout:get("test_processor", {2012, 8, 2}, {2014, 8, 3}),
    statout ! write_cache,
    timer:sleep(300),
    Res2 = statout:get("test_processor", {2012, 8, 2}, {2014, 8, 3}),
    application:stop(statist),
    os:cmd("rm -rf __data_temp"),
    ?assert(Res1 =:= 3),
    ?assert(Res2 =:= 3).

test_processor() ->
    #statprocessor{name = "test processor", version = "1",
                   low_func = fun(P, A) ->
                                      case proplists:get_value(<<"hello">>, P) of
                                          <<"world">> -> A + 1;
                                          _ -> A
                                      end
                              end,
                   low_init = 0,
                   high_func = fun(Init, Ress) -> lists:foldl(fun(X, A) -> X + A end, Init, Ress) end,
                   high_init = 0
                  }.
