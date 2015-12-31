module Etl

open FSharp.Data
open FSharp.Data.Sql

// CSV Input File
type DataTypesTest = CsvProvider< "data/MSFT.csv">
let InputFile = DataTypesTest.Load("data/MSFT.csv")

// Target Database
[<Literal>]
let connStr = "User ID=jcasal;Host=localhost;Port=5432;Database=etl;"

[<Literal>]
let resolutionFolder = @"/Users/jcasal/Code/fscript/packages/Npgsql.2.2.7/lib/net45"
        
type targetSql = SqlDataProvider<Common.DatabaseProviderTypes.POSTGRESQL, connStr, resolutionFolder, UseOptionTypes=true>
let DbContext = targetSql.GetDataContext()

// Load
for row in InputFile.Rows do
    let newitem = DbContext.Public.Item.Create()
    newitem.High <- Some row.High
    newitem.Close <- Some row.Close
    newitem.Date <- row.Date
    newitem.Low <- Some row.Low
    newitem.Open <- Some row.Open
    newitem.AdjClose <- Some row.``Adj Close``

DbContext.SubmitUpdates()
