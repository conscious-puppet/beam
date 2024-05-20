- make the where clause syntax compatible with prepared statement
    - binary operators
```
qBinOpE :: BeamSqlBackend be
        => (BeamSqlBackendExpressionSyntax be ->
            BeamSqlBackendExpressionSyntax be ->
            BeamSqlBackendExpressionSyntax be)
        -> QGenExpr context be s a -> QGenExpr context be s b
        -> QGenExpr context be s c
qBinOpE mkOpE (QExpr a) (QExpr b) = QExpr (mkOpE <$> a <*> b)
```

- the following makes this exp `SELECT "t0"."id" ASA "res0", "t0"."key" ASA "res1"`
```
instance IsSql92ProjectionSyntax PgProjectionSyntax where
  type Sql92ProjectionExpressionSyntax PgProjectionSyntax = PgExpressionSyntax

  projExprs exprs =
    PgProjectionSyntax $
    pgSepBy (emit ", ")
            (map (\(expr, nm) -> fromPgExpression expr <>
                                 maybe mempty (\nm -> emit " AS " <> pgQuotedIdentifier nm) nm) exprs)
```


1. [ ] change sqlsyntax92 to create parametrised query
2. [ ] copy that syntax into sqlpreparedsyntax
3. [ ] create another sqlBuildQuery when the prepared is true?
