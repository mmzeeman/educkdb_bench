
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
    {ok, Conn} = esqlite3:connect(Db),

    [] = esqlite3:q("create table bench (a int, b int, c int, d int, e int, f int, g int, h int, i int, j int)", Conn),

    Values = lists:seq(1, 10),
    Range = lists:seq(1, 100_000),

    %% Insert 1_000_000 records
    {Time, _} = timer:tc(fun() ->
                                 ok = educkdb:query(Conn, "begin;"),

                                 {ok, Stmt} = educkdb:prepare("insert into bench values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?);", Conn),
                                 lists:foreach(fun(_) ->
                                                       [ ok = educkdb:bind_integer(Conn, V, V) || V <- Values ],
                                                       ok = educkdb_prepare:bind_integer(Stmt, Values),
                                                       ok = educkdb:execute_prepared(Stmt)
                                               end,
                                               Range),

                                 ok = educkdb:query(Conn, "commit;")
                         end),

    io:fwrite("100_000 inserts took: ~p milliseconds~n", [Time/1000]),

    %% Select 100_000 
    {SelectTime, Rows} = timer:tc(fun() ->
                                          educkdb:query(Conn, "select * from bench;")
                                  end),
    
    io:fwrite("Select of ~p rows took: ~p milliseconds~n", [length(Rows), SelectTime/1000]),

    ok.


