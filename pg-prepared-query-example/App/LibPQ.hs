module LibPQ (main) where

import Control.Monad (unless)
import Database.PostgreSQL.LibPQ
import System.Environment (getEnv)
import System.Exit (exitFailure)

import qualified Data.ByteString.Char8 as BS8
import Data.Maybe (fromMaybe)
import Data.String (IsString (fromString))
import qualified Database.PostgreSQL.LibPQ as LibPQ
import qualified Database.PostgreSQL.Simple.TypeInfo.Static as PQ

main :: IO ()
main = do
  libpqVersion >>= print
  connectionString <- getEnv "DATABASE_CONNSTRING"
  connection <- connectdb (BS8.pack connectionString)
  smoke connection
  executeQueries connection
  executeParamQueries connection
  executePreparedQueries connection
  finish connection

smoke :: Connection -> IO ()
smoke conn = do
  -- status functions
  db conn >>= print
  user conn >>= print
  host conn >>= print
  port conn >>= print
  status conn >>= print
  transactionStatus conn >>= print
  protocolVersion conn >>= print
  serverVersion conn >>= print

  s <- status conn
  unless (s == ConnectionOk) exitFailure

executeQueries :: Connection -> IO ()
executeQueries conn = do
  result <- exec conn (fromString "select * from \"gcp_Configurations\"")
  status <- traverse resultStatus result
  numOfRows <- traverse ntuples result
  numOfCols <- traverse nfields result
  print result
  print status
  print numOfRows
  print numOfCols
  foldl
    ( \acc i -> do
        acc >> maybe (print "Empty Result") (\r -> print =<< fname r i) result
    )
    (print "print column names:")
    [0 .. (fromMaybe (Col 0) numOfCols - Col 1)]

  foldl
    ( \acc i -> do
        acc >> maybe (print "Empty Result") (\r -> print =<< fformat r i) result
    )
    (print "print formats:")
    [0 .. (fromMaybe (Col 0) numOfCols - Col 1)]

  foldl
    ( \acc i -> do
        acc >> maybe (print "Empty Result") (\r -> print =<< ftype r i) result
    )
    (print "print type:")
    [0 .. (fromMaybe (Col 0) numOfCols - Col 1)]

  let rc = [(r, c) | r <- [0 .. (fromMaybe (Row 0) numOfRows - Row 1)], c <- [0 .. (fromMaybe (Col 0) numOfCols - Col 1)]]

  foldl
    ( \acc (ro, col) -> do
        acc
          >> maybe
            (print "Empty Result")
            ( \r -> do
                colName <- fname r col
                colValue <- getvalue' r ro col
                print $ colName <> Just ": " <> colValue
            )
            result
    )
    (print "print value:")
    rc

executeParamQueries :: Connection -> IO ()
executeParamQueries conn = do
  result <- execParams conn (fromString "select * from \"gcp_Configurations\" where key=$1 limit 1;") [Just (PQ.textOid, "PassettoKeyId", LibPQ.Binary)] LibPQ.Text
  status <- traverse resultStatus result
  numOfRows <- traverse ntuples result
  numOfCols <- traverse nfields result
  print result
  print status
  print numOfRows
  print numOfCols

  let rc = [(r, c) | r <- [0 .. (fromMaybe (Row 0) numOfRows - Row 1)], c <- [0 .. (fromMaybe (Col 0) numOfCols - Col 1)]]

  foldl
    ( \acc (ro, col) -> do
        acc
          >> maybe
            (print "Empty Result")
            ( \r -> do
                colName <- fname r col
                colValue <- getvalue' r ro col
                print $ colName <> Just ": " <> colValue
            )
            result
    )
    (print "print value:")
    rc

executePreparedQueries :: Connection -> IO ()
executePreparedQueries conn = do
  let queryName ="gcp_Configurations_key"
  preparedQueryResult <- prepare conn queryName "select * from \"gcp_Configurations\" where key=$1 limit 1;" (Just [PQ.textOid])
  preparedQueryResultStatus <- traverse resultStatus preparedQueryResult
  result <- execPrepared conn queryName [Just ("PassettoKeyId", LibPQ.Binary)] LibPQ.Text
  status <- traverse resultStatus result
  numOfRows <- traverse ntuples result
  numOfCols <- traverse nfields result
  print preparedQueryResult
  print preparedQueryResultStatus
  print result
  print status
  print numOfRows
  print numOfCols

  let rc = [(r, c) | r <- [0 .. (fromMaybe (Row 0) numOfRows - Row 1)], c <- [0 .. (fromMaybe (Col 0) numOfCols - Col 1)]]

  foldl
    ( \acc (ro, col) -> do
        acc
          >> maybe
            (print "Empty Result")
            ( \r -> do
                colName <- fname r col
                colValue <- getvalue' r ro col
                print $ colName <> Just ": " <> colValue
            )
            result
    )
    (print "print value:")
    rc
