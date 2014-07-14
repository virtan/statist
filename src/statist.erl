-module(statist).
-behaviour(gen_server).

-export([
          start/1,
          start_link/1,
          stop/0,

          event/1,

          init/1,
          handle_call/3,
          handle_cast/2,
          handle_info/2,
          code_change/3,
          terminate/2
        ]).

-record(state, {
          file1 = undefined,
          file2 = undefined
          %% TODO: define internal state
        }).


%% Exported

start(Options) ->
    gen_server:start({local, ?MODULE}, ?MODULE, Options, []).

start_link(Options) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, Options, []).

stop() ->
    gen_server:call(?MODULE, stop).

event(Map) ->
    gen_server:cast(?MODULE, {event, os:timestamp(), Map}).


%% Internal

init(_Options) ->
    %% TODO: process Options, prepare state
    {ok, #state{}}.


handle_call(stop, _From, #state{} = State) ->
    {stop, normal, stopped, State};

handle_call(Unexpected, _From, #state{} = State) ->
    {stop, {error_unexpected, Unexpected}, error_unexpected, State}.


handle_cast({event, TimeStamp, Map}, #state{} = State) ->
    {FileToWrite, State1} = choose_file_to_write(TimeStamp, State),
    MapSerialized = serialize_map(TimeStamp, Map),
    file:write(FileToWrite, MapSerialized),
    {noreply, State1};

handle_cast(Unexpected, #state{} = State) ->
    {stop, {error_unexpected, Unexpected}, State}.


handle_info(command, #state{} = State) ->
    {noreply, State};

handle_info(Unexpected, #state{} = State) ->
    {stop, {error_unexpected, Unexpected}, State}.


code_change(_OldVsn, #state{} = State, _Extra) ->
    {ok, State}.


terminate({error_unexpected, _Unexpected}, #state{} = State) ->
    %% TODO: report about unexpected
    close_files(State),
    ok;

terminate(_Reason, #state{} = State) ->
    close_files(State),
    ok.


serialize_map({_, _, Micro} = TimeStamp, Map) ->
    {_, {_, Min, Sec}} = calendar:now_to_local_time(TimeStamp),
    iolist_to_binary([
      (integer_to_list(Min))/binary, ":",
      (integer_to_list(Sec))/binary, ".",
      (integer_to_list(Micro))/binary, " ",
      (map_to_string(Map))/binary, "\n"
                     ]).


map_to_string(Map) ->
    map_to_string(Map, []).

map_to_string([], Output) ->
    lists:reverse(Output);

%% 23:12.123654 type=calculation user=virtan ip=12.13.14.15 text=hello\ world
map_to_string([{Key, Value} | Rest], Output) ->
    map_to_string(Rest, [[$ , safe_binary(Key), $=, safe_binary(Value)] | Output]).


%% Escape with backslash: space, =, \
%% Newline becomes \n
safe_binary(Binary) ->
    re:replace(
      re:replace(Binary, <<"( |=|\\\\)">>, <<"\\\\&">>, [global]),
      <<"\n">>, <<"\\n">>, [global]).


choose_file_to_write(TimeStamp, #state{file1 = File1, file2 = File2} = State) ->
    {{Year, Month, Day}, {Hour, _, _}} = calendar:now_to_local_time(TimeStamp),
    Fn = integer_to_list(Year) ++ "/stat." ++
         two_digits(integer_to_list(Month)) ++ two_digits(integer_to_list(Day)) ++
         "." ++ two_digits(integer_to_list(Hour)) ++ ".data",
    case {File1, File2} of
        {{Fn, Fd, _}, _} ->
            {Fd, State#state{file1 = {Fn, Fd, os:timestamp()}}};
        {_, {Fn, Fd, _}} ->
            {Fd, State#state{file2 = {Fn, Fd, os:timestamp()}}};
        {undefined, _} ->
            Fd = open_file(Fn),
            {Fd, State#state{file1 = {Fn, Fd, os:timestamp()}}};
        {_, undefined} ->
            Fd = open_file(Fn),
            {Fd, State#state{file2 = {Fn, Fd, os:timestamp()}}};
        {{Fn, Fd, AccessTime1}, {_, _, AccessTime2}} when AccessTime1 < AccessTime2 ->
            close_file(Fd),
            Fd1 = open_file(Fn),
            {Fd1, State#state{file1 = {Fn, Fd1, os:timestamp()}}};
        {{_, _, AccessTime1}, {Fn, Fd, AccessTime2}} when AccessTime1 >= AccessTime2 ->
            close_file(Fd),
            Fd1 = open_file(Fn),
            {Fd1, State#state{file2 = {Fn, Fd1, os:timestamp()}}}
    end.


two_digits([_] = OneDigit) -> [$0 | OneDigit];
two_digits(MoreThanOneDigit) -> MoreThanOneDigit.


open_file(FName) ->
    file:make_dir(filename:dirname(FName)),
    {ok, Fd} = file:open(FName, [write, append, binary, compressed]),
    Fd.


close_file(Fd) ->
    file:close(Fd).


close_files(#state{file1 = File1, file2 = File2}) ->
    case File1 of
        {_, Fd1, _} ->
            close_file(Fd1);
        _ ->
            do_nothing
    end,
    case File2 of
        {_, Fd2, _} ->
            close_file(Fd2);
        _ ->
            do_nothing
    end.
