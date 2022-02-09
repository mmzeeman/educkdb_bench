
-module(educkdb_bench).

-export([main/0, ets/0]).

ets() ->
    Ref = ets:new(bench, [ordered_set, {keypos, 1}]),

    Values = lists:seq(1, 10),
    Range = lists:seq(1, 100_000),

    {Time, _} = timer:tc(fun() ->
                                 lists:foreach(fun(I) ->
                                                       ets:insert(Ref, list_to_tuple([I] ++ Values))
                                               end,
                                               Range)
                         end),

    io:fwrite("100_000 inserts took: ~p milliseconds~n", [Time/1000]),

    ok.


main() ->
    {ok, Db} = educkdb:open(":memory:"),
    {ok, Conn} = educkdb:connect(Db),

    {ok, _, []} = q(Conn, "create table bench (a int, b int, c int, d int, e int, f int, g int, h int, i int, j int)"),

    Values = lists:seq(1, 10),
    Range = lists:seq(1, 100_000),

    %% Insert 1_000_000 records
    {Time, _} = timer:tc(fun() ->
                                 {ok, [], []} = q(Conn, "begin;"),

                                 {ok, Stmt} = educkdb:prepare(Conn, "insert into bench values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"),
                                 lists:foreach(fun(_) ->
                                                       [ ok = educkdb:bind_int32(Stmt, V, V) || V <- Values ],
                                                       {ok, _} = educkdb:execute_prepared(Stmt)
                                               end,
                                               Range),

                                 {ok, [], []} = q(Conn, "commit;")
                         end),

    io:fwrite("100_000 inserts took: ~p milliseconds~n", [Time/1000]),

    %% Select 100_000 
    {SelectTime, {ok, _, Rows}} = timer:tc(fun() ->
                                                   q(Conn, "select * from bench;")
                                           end),
    
    io:fwrite("Select of ~p rows took: ~p milliseconds~n", [length(Rows), SelectTime/1000]),

    ok.


q(Conn, Query) ->
    case educkdb:query(Conn, Query) of
        {ok, Result} -> educkdb:extract_result(Result);
        {error, _}=E -> E
    end.

x(Stmt) ->
    case educkdb:execute_prepared(Stmt) of
        {ok, Result} -> educkdb:extract_result(Result);
        {error, _}=E -> E
    end.

