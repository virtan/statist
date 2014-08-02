-module(statpartition).
-export([
         filepath_for_ts/1,
         filepaths_for_interval/2
        ]).

% TimeStamp is unixtime in now() format
filepath_for_ts(TimeStamp) ->
    {Day, _} = calendar:now_to_local_time(TimeStamp),
    day_to_filepath(Day).

day_to_filepath({Year, Month, Day}) ->
    filename:join([application:get_env(statist, base_dir, "data"),
                   integer_to_list(Year),
                   "stat." ++ vutil:number_format(integer_to_list(Month), 2)
                   ++ vutil:number_format(integer_to_list(Day), 2) ++ ".data"]).

% From and To is of format {Y, M, D}
filepaths_for_interval({Y, M, _} = From, To) ->
    filepaths_for_interval(From, To, calendar:last_day_of_the_month(Y, M), []).

filepaths_for_interval({Y, M, D}, {Y, M, D} = To, _, Res) ->
    Res1 = [day_to_filepath(To) | Res],
    lists:reverse(Res1);
filepaths_for_interval({Y1, 12, D1} = From, To, D1, Res) ->
    filepaths_for_interval({Y1 + 1, 1, 1}, To, calendar:last_day_of_the_month(Y1 + 1, 1),
                           [day_to_filepath(From) | Res]);
filepaths_for_interval({Y1, M1, D1} = From, To, D1, Res) ->
    filepaths_for_interval({Y1, M1 + 1, 1}, To, calendar:last_day_of_the_month(Y1, M1 + 1),
                           [day_to_filepath(From) | Res]);
filepaths_for_interval({Y1, M1, D1} = From, To, D2, Res) ->
    filepaths_for_interval({Y1, M1, D1 + 1}, To, D2, [day_to_filepath(From) | Res]).
