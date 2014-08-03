-module(statformat).

-export([
         serialize_map/2,
         deserialize_map/1
        ]).

-include_lib("eunit/include/eunit.hrl").

serialize_map({_, _, Micro} = TimeStamp, Map) ->
    {_, {Hour, Min, Sec}} = calendar:now_to_local_time(TimeStamp),
    iolist_to_binary([
      vutil:number_format(integer_to_list(Hour), 2), ":",
      vutil:number_format(integer_to_list(Min), 2), ":",
      vutil:number_format(integer_to_list(Sec), 2), ".",
      vutil:number_format(integer_to_list(Micro), 6), " ",
      map_to_string(Map), "\n"
                     ]).

deserialize_map(Serialized) ->
    [HourBinary, MinBinary, SecBinary, MicroBinary, SerializedMap]
        = re:split(Serialized, <<"[:. ]">>, [{parts, 5}]),
    Hour = list_to_integer(binary_to_list(HourBinary)),
    Min = list_to_integer(binary_to_list(MinBinary)),
    Sec = list_to_integer(binary_to_list(SecBinary)),
    Micro = list_to_integer(binary_to_list(MicroBinary)),
    Cut = binary:longest_common_suffix([SerializedMap, <<"\n">>]),
    [{timestamp, {{Hour, Min, Sec}, Micro}}
     | string_to_map(binary:part(SerializedMap, 0, size(SerializedMap) - Cut))].

map_to_string(Map) ->
    map_to_string(Map, []).

map_to_string([], Output) ->
    lists:reverse(Output);

%% 23:12.123654 type=calculation user=virtan ip=12.13.14.15 text=hello\ world
map_to_string([{Key, Value} | Rest], Output) ->
    map_to_string(Rest, [[$ , safe_binary(vutil:any_to_binary(Key)),
                          $=, safe_binary(vutil:any_to_binary(Value))]
                         | Output]).


string_to_map(Serialized) ->
    lists:map(fun(Pair) ->
                      [KeySerialized, ValueSerialized]
                            = re:split(Pair, <<"(?<!\\\\)=">>, [{parts, 2}]),
                      {restore_binary(KeySerialized), restore_binary(ValueSerialized)}
              end, lists:delete(<<>>, re:split(Serialized, <<"(?<!\\\\) ">>))).

%% Escape with backslash: space, =, \
%% Newline becomes \n
safe_binary(Binary) ->
    re:replace(
      re:replace(Binary, <<"( |=|\\\\)">>, <<"\\\\&">>, [global]),
      <<"[\n]">>, <<"\\\\n">>, [global]).


restore_binary(Serialized) ->
    iolist_to_binary(
      re:replace(
        re:replace(Serialized, <<"\\\\n">>, <<"\n">>, [global]),
        <<"\\\\(.)">>, <<"\\1">>, [global])).


% Tests

serialization_test() ->
    Now = now(),
    {_, {Hour, Min, Sec}} = calendar:now_to_local_time(Now),
    {_, _, Micro} = Now,
    Serialized = serialize_map(Now, [{hello, "world"}, {<<" hello\n\\ ">>, 12.13}]),
    ?assert([{timestamp, {{Hour, Min, Sec}, Micro}}, {<<"hello">>, <<"world">>}, {<<" hello\n\\ ">>, vutil:any_to_binary(12.13)}] =:= deserialize_map(Serialized)).
