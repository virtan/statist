-module(statmr).
-export([
         map/2,
         reduce/2
        ]).

-record(statmr, {
          name,
          map,
          map_init,
          reduce,
          reduce_init
         }).

map(#statmr{map = MapFunc, map_init = MapInit}, File) ->
    process_file(MapFunc, MapInit, File).


reduce(#statmr{reduce = ReduceFunc, reduce_init = ReduceInit}, MapResults) ->
    ReduceFunc(ReduceInit, MapResults).


process_file(Fun, Acc, File) ->
    {ok, FD} = file:open(File, [read, read_ahead, binary]),
    Res = process_lines(Fun, Acc, FD),
    file:close(FD),
    Res.

process_lines(Fun, Acc, FD) ->
    case file:read_line(FD) of
        {ok, Data} ->
                try
                  Map = statformat:deserialize_map(Data),
                  process_lines(Fun, Fun(Map, Acc), FD)
                catch
                    % TODO: print to log
                    _:_ -> process_lines(Fun, Acc, FD)
                end;
        _ -> Acc
    end.

