-module(statprocessor).
-export([
         processor_module/1,
         has_processor/1,
         get_processor/1,
         low/2,
         high/2,
         low_key/1
        ]).

-include_lib("statist/include/statprocessor.hrl").

processor_module(ModuleName) ->
    application:set_env(statist, processors, ModuleName).


low(Processor, File) when is_list(Processor) ->
    low(list_to_binary(Processor), File);
low(Processor, File) when is_binary(Processor) ->
    low(get_processor(Processor), File);
low(#statprocessor{low_func = LowFunc, low_init = LowInit}, File) ->
    process_file(LowFunc, LowInit, File).


high(Processor, LowResults) when is_list(Processor) ->
    high(list_to_binary(Processor), LowResults);
high(Processor, LowResults) when is_binary(Processor) ->
    high(get_processor(Processor), LowResults);
high(#statprocessor{high_func = HighFunc, high_init = HighInit}, LowResults) ->
    HighFunc(HighInit, LowResults).


get_processor(ProcessorName) when is_list(ProcessorName) ->
    get_processor(list_to_binary(ProcessorName));
get_processor(ProcessorName) when is_binary(ProcessorName) ->
    case application:get_env(statist, processors, undefined) of
        undefined ->
            throw("no such processor");
        Defined ->
            case vutil:has_exported_function(Defined, ProcessorName, 0) of
                true -> 
                    Func = list_to_atom(binary_to_list(ProcessorName)),
                    Defined:Func();
                _ -> throw("no such processor")
            end
    end.

has_processor(ProcessorName) ->
    try
        get_processor(ProcessorName),
        true
    catch
        _:_ -> false
    end.


low_key(#statprocessor{low_func = Func, low_init = Init}) ->
    erlang:phash2({Func, Init}).


process_file(Fun, Acc, File) ->
    case file:open(File, [read, read_ahead, binary]) of
        {ok, FD} ->
            Res = process_lines(Fun, Acc, FD),
            file:close(FD),
            Res;
        _ -> Acc
    end.

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

