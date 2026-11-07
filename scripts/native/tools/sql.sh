#!/bin/bash
# usage: sql.sh <db> "<sql>"...   (JDBC helper: the Postgres bundle used here has no psql)
db=$1; shift
NATIVE_WIN=${PAYFLOW_NATIVE:-C:\\payflow-native}
J="$HOME/scoop/apps/temurin25-jdk/current/bin/java.exe"
"$J" -cp "$NATIVE_WIN\\tools\\pg.jar" "$NATIVE_WIN\\tools\\Sql.java" "jdbc:postgresql://127.0.0.1:55432/$db" "$@"
