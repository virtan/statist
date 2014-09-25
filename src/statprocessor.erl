-module(statprocessor).
-export([
         processor_module/1,
         has_processor/2,
         get_processor/2,
         low/3,
         high/2,
         low_key/1,
         runner/1
        ]).

-include_lib("statist/include/statprocessor.hrl").

processor_module(ModuleName) ->
    application:set_env(statist, processors, ModuleName).


low(#statprocessor{low_func = LowFunc, low_init = LowInit}, Date, File) ->
    process_file(LowFunc, LowInit, Date, File).


high(#statprocessor{high_func = HighFunc, high_init = HighInit}, LowResults) ->
    HighFunc(HighInit, LowResults).


get_processor(ProcessorName, ProcInit) when is_list(ProcessorName) ->
    get_processor(list_to_binary(ProcessorName), ProcInit);
get_processor(ProcessorName, ProcInit) when is_binary(ProcessorName) ->
    case application:get_env(statist, processors, undefined) of
        undefined ->
            throw("no such processor");
        Defined ->
            case vutil:has_exported_function(Defined, ProcessorName, 1) of
                true -> 
                    Func = list_to_atom(binary_to_list(ProcessorName)),
                    Defined:Func(ProcInit);
                _ -> throw("no such processor")
            end
    end.

has_processor(ProcessorName, ProcInit) ->
    try
        get_processor(ProcessorName, ProcInit),
        true
    catch
        error:undef -> false;
        throw:"no such processor" -> false
    end.


low_key(#statprocessor{low_func = Func, low_init = Init}) ->
    erlang:phash2({Func, Init}).


runner(#statprocessor{runner = Runner}) ->
    case Runner of
        foreground -> fun(_, F) -> F() end;
        F when is_function(F, 2) -> F;
        _ -> throw("unknown runner")
    end.


process_file(Fun, Acc, Date, File) ->
    case file:open(File, [read, read_ahead, binary]) of
        {ok, FD} ->
            Res = process_lines(Fun, Acc, Date, FD),
            file:close(FD),
            Res;
        _ -> Acc
    end.

process_lines(Fun, Acc, Date, FD) ->
    case file:read_line(FD) of
        {ok, Data} ->
                try
                  Map = statformat:deserialize_map(Date, Data),
                  process_lines(Fun, Fun(Map, Acc), Date, FD)
                catch
                    % TODO: print to log
                    _:_ -> process_lines(Fun, Acc, Date, FD)
                end;
        _ -> Acc
    end.

