--*************************************************************************--
-- Title: Incremental Loading Example
-- Author: RRoot
--Desc: This file gives an example of how you might code an incremetal ETL process with SQL code
-- Change Log: When,Who,What
-- 2018-01-24,RRoot,Created File
-- 2020-04-24,RRoot,Added Merge Example
-- 2022-12-29,RRoot,Added Transaction Statements
--**************************************************************************--

If Exists(Select name from master.dbo.sysdatabases Where Name = 'Hint1DB')
Begin
    USE [master];
    ALTER DATABASE [Hint1DB] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE [Hint1DB];
End;
Go
Create Database Hint1DB; 
Go
USE Hint1DB;
Go
SELECT 
 [ProductID]
,[ProductName]
,[CategoryID]
INTO [Products]
FROM [Northwind].[dbo].[Products];
Go
SELECT 
 [CategoryID]
,[CategoryName]
INTO [Categories]
FROM [Northwind].[dbo].[Categories];
Go


USE [master]
GO
If Exists (Select * from Sysdatabases Where Name = 'DWHint1DB_withSCD')
    Begin 
        ALTER DATABASE [DWHint1DB_withSCD] SET SINGLE_USER WITH ROLLBACK IMMEDIATE
        DROP DATABASE [DWHint1DB_withSCD]
    End
GO
Create Database [DWHint1DB_withSCD]
Go
USE [DWHint1DB_withSCD]
Go
CREATE TABLE DWHint1DB_withSCD.dbo.DimProducts(
     ProductKey int	IDENTITY   		   NOT NULL
    ,ProductID int			   		   NOT NULL
    ,ProductName nVarchar(100) 		   NOT NULL
    ,ProductCategoryID int	   		   NOT NULL
    ,ProductCategoryName nVarchar(100) NOT NULL 
    ,StartDate int			   		   NOT NULL
    ,EndDate int			  		   NULL
    ,IsCurrent char(3)		  		   NOT NULL
    CONSTRAINT PK_DimProducts PRIMARY KEY (ProductKey)
)
Go

--********************************************************************--
-- Create the ETL Views and Stored Procedures
--********************************************************************--
go 
Create View vETLDimProducts
/* Author: RRoot
** Desc: Extracts and transforms data for DimProducts
** Change Log: When,Who,What
** 20189-01-17,RRoot,Created view.
*/
As
  SELECT
    [ProductID] = p.ProductID
   ,[ProductName] = CAST(p.ProductName as nVarchar(100))
   ,[ProductCategoryID] = p.CategoryID
   ,[ProductCategoryName] = CAST(c.CategoryName as nVarchar(100))
  FROM [Hint1DB].dbo.Categories as c
  INNER JOIN [Hint1DB].dbo.Products as p
  ON c.CategoryID = p.CategoryID;
go
/* Testing Code:
 Select * From vETLDimProducts;
*/

go
Create Procedure pETLSyncDimProducts
/* Author: RRoot
** Desc: Updates data in DimProducts using the vETLDimProducts view
** Change Log: When,Who,What
** 20189-01-17,RRoot,Created Sproc.
*/
AS
 Begin
  Declare @ReturnCode int = 0;
  Begin Try
    -- ETL Processing Code --

    Begin Tran;
      -- 1) For UPDATE: Change the EndDate and IsCurrent on any added rows
      -- NOTE: Performing the Update before an Insert makes the coding eaiser since there is only one current version of the data      
      With ChangedProducts 
      As(
         Select ProductID, ProductName, ProductCategoryID, ProductCategoryName From vETLDimProducts
         Except
         Select ProductID, ProductName, ProductCategoryID, ProductCategoryName From DimProducts
           Where IsCurrent = 1 -- Needed if the value is changed back to previous value
        ) UPDATE [DWHint1DB_withSCD].dbo.DimProducts 
           SET EndDate = Cast(Convert(nvarchar(50), GetDate(), 112) as int)
              ,IsCurrent = 0
           WHERE ProductID IN (Select ProductID From ChangedProducts)
           ;

      -- 2)For INSERT or UPDATES: Add new rows to the table
      With AddedORChangedProducts 
        As(
            Select ProductID, ProductName, ProductCategoryID, ProductCategoryName From vETLDimProducts
            Except
            Select ProductID, ProductName, ProductCategoryID, ProductCategoryName From DimProducts
              Where IsCurrent = 1 -- Needed if the value is changed back to previous value
          ) INSERT INTO [DWHint1DB_withSCD].dbo.DimProducts
            ([ProductID],[ProductName],[ProductCategoryID],[ProductCategoryName],[StartDate],[EndDate],[IsCurrent])
            SELECT
              [ProductID]
             ,[ProductName]
             ,[ProductCategoryID]
             ,[ProductCategoryName]
             ,[StartDate] = Cast(Convert(nvarchar(50), GetDate(), 112) as int)
             ,[EndDate] = Null
             ,[IsCurrent] = 1
            FROM vETLDimProducts
            WHERE ProductID IN (Select ProductID From AddedORChangedProducts)
            ;

      -- 3) For Delete: Change the IsCurrent status to zero
      With DeletedProducts 
          As(
              Select ProductID, ProductName, ProductCategoryID, ProductCategoryName From DimProducts
                Where IsCurrent = 1 -- We do not care about row already marked zero!
              Except            			
              Select ProductID, ProductName, ProductCategoryID, ProductCategoryName From vETLDimProducts
            ) UPDATE [DWHint1DB_withSCD].dbo.DimProducts 
                SET EndDate = Cast(Convert(nvarchar(50), GetDate(), 112) as int)
                   ,IsCurrent = 0
                WHERE ProductID IN (Select ProductID From DeletedProducts)
                ;
     Commit Tran;
    --( add logging code here )--
    Set @ReturnCode = 1; -- success code
  End Try
  Begin Catch
    If @@TRANCOUNT > 0 Rollback Tran;
    Print Error_Message()
    Set @ReturnCode = -1
  End Catch
  Return @ReturnCode;
 End
go

/************************** Test Your ETL Process! **************************/

-- Test Full Sync:
Select * From Hint1DB.dbo.Products Order By ProductID Desc
Select * From DWHint1DB_withSCD.dbo.DimProducts Order By ProductID Desc
go

Exec pETLSyncDimProducts;
Select * From Hint1DB.dbo.Products Order By ProductID Desc
Select * From DWHint1DB_withSCD.dbo.DimProducts Order By ProductID Desc
go

-- Test Insert:
Insert Into Hint1DB.dbo.Products (ProductName, CategoryID) Values ('AAA', 1) 
Select * From Hint1DB.dbo.Products Order By ProductID Desc
Select * From DimProducts Order By ProductID Desc

Exec pETLSyncDimProducts;
Select * From Hint1DB.dbo.Products Order By ProductID Desc
Select * From DimProducts Order By ProductID Desc
go

-- Test Update:
Update Hint1DB.dbo.Products Set ProductName = 'BBB' Where ProductName = 'AAA' 
Select * From Hint1DB.dbo.Products Order By ProductID Desc
Select * From DimProducts Order By ProductID Desc
go

Exec pETLSyncDimProducts;
Select * From Hint1DB.dbo.Products Order By ProductID Desc
Select * From DimProducts Order By ProductID Desc
go

-- Test Update back to Previous Value:
Update Hint1DB.dbo.Products Set ProductName = 'AAA' Where ProductName = 'BBB' 
Select * From Hint1DB.dbo.Products Order By ProductID Desc
Select * From DimProducts Order By ProductID Desc
go

Exec pETLSyncDimProducts;
Select * From Hint1DB.dbo.Products Order By ProductID Desc
Select * From DimProducts Order By ProductID Desc
go

-- Test Delete: 
Delete From Hint1DB.dbo.Products Where  ProductName = 'AAA'
Select * From Hint1DB.dbo.Products Order By ProductID Desc
Select * From DimProducts Order By ProductID Desc
go

Exec pETLSyncDimProducts;
Select * From Hint1DB.dbo.Products Order By ProductID Desc
Select * From DimProducts Order By ProductID Desc
go


/************************** Completing this with Merge Instead **************************/
go
Alter Procedure pETLSyncDimProducts
/* Author: RRoot
** Desc: Updates data in DimProducts using the vETLDimProducts view
** Change Log: When,Who,What
** 2020-01-01,RRoot,Created Sproc.
*/
As
Begin
  Declare @ReturnCode int = 0;
  Begin Try
    -- ETL Processing Code --
    Begin Tran;
      Merge Into DimProducts as t
       Using vETLDimProducts as s -- For Merge to work with SCD tables, I need to insert a new row when the following is not true:
        On  t.ProductID = s.ProductID
        And t.ProductName = s.ProductName
        And t.ProductCategoryID = s.ProductCategoryID
        And t.ProductCategoryName = s.ProductCategoryName 
       When Not Matched -- At least one column value does not match add a new row:
        Then
         Insert (ProductID, ProductName, ProductCategoryID, ProductCategoryName, StartDate,EndDate,IsCurrent)
          Values (s.ProductID
                ,s.ProductName
                ,s.ProductCategoryID
                ,s.ProductCategoryName
                ,Cast(Convert(nvarchar(100), GetDate(), 112) as int) -- Smart Key can be joined to the DimDate
                ,Null
                ,'Yes')
        When Not Matched By Source -- If there is a row in the target (dim) table that is no longer in the source table
         Then -- indicate that row is no longer current
          Update 
           Set t.EndDate = Cast(Convert(nvarchar(100), GetDate(), 112) as int) -- Smart Key can be joined to the DimDate
              ,t.IsCurrent = 'No'
              ;
    Commit Tran;
    --( add logging code here )--
    Set @ReturnCode = 1; -- success code
  End Try
  Begin Catch
    If @@TRANCOUNT > 0 Rollback Tran;
    Print Error_Message()
    Set @ReturnCode = -1
  End Catch
  Return @ReturnCode;
End
go

/************************** Test Your ETL Process! **************************/

-- Test Full Sync:
Truncate Table DimProducts;
go
Select Top 2 * From vETLDimProducts Order By ProductID Desc
Select Top 2 * From DWHint1DB_withSCD.dbo.DimProducts Order By ProductID Desc
go

Exec pETLSyncDimProducts;
Select Top 2 * From vETLDimProducts Order By ProductID Desc
Select * From DWHint1DB_withSCD.dbo.DimProducts Order By ProductID Desc
go

-- Test Insert with Merge:
Insert Into Hint1DB.dbo.Categories(CategoryName) Values ('CatA') 
Select Top 2 * From Hint1DB.dbo.Categories Order By CategoryID Desc
Select Top 2 * From DimProducts Order By ProductID Desc

Insert Into Hint1DB.dbo.Products (ProductName, CategoryID) Values ('AAA', 9) 
Select Top 2 * From vETLDimProducts Order By ProductID Desc
Select Top 2 * From DimProducts Order By ProductID Desc

Exec pETLSyncDimProducts;
Select Top 2 * From vETLDimProducts Order By ProductID Desc
Select Top 2 * From DimProducts Order By ProductID Desc
go

-- Test Update with Merge:
Update Hint1DB.dbo.Categories Set CategoryName = 'CatB' Where CategoryName = 'CatA' 
Select Top 2 * From Hint1DB.dbo.Categories Order By CategoryID Desc
Select Top 2 * From vETLDimProducts Order By ProductID Desc
Select Top 2 * From DimProducts Order By ProductID Desc
go

Exec pETLSyncDimProducts;
Select Top 2 * From vETLDimProducts Order By ProductID Desc
Select Top 2 * From DimProducts Order By ProductID Desc
go

Update Hint1DB.dbo.Products Set ProductName = 'BBB' Where ProductName = 'AAA' 
Select Top 2 * From vETLDimProducts Order By ProductID Desc
Select Top 2 * From DimProducts Order By ProductID Desc
go

Exec pETLSyncDimProducts;
Select Top 2 * From vETLDimProducts Order By ProductID Desc
Select Top 3 * From DimProducts Order By ProductID Desc
go

-- Test Delete with Merge:
Delete From Hint1DB.dbo.Products Where  ProductName = 'AAA'
Select Top 2 * From Hint1DB.dbo.Products Order By ProductID Desc
Select Top 3 * From DimProducts Order By ProductID Desc
go

Exec pETLSyncDimProducts;
Select Top 2 * From Hint1DB.dbo.Products Order By ProductID Desc
Select Top 3 * From DimProducts Order By ProductID Desc
go


/************************** Dealing with Previous Value Issue **************************/
-- Issue: When you change the values back to an earlier value the data matches an earlier row, so you do not get another insert!

Insert Into Hint1DB.dbo.Products (ProductName, CategoryID) Values ('CCC', 9) 
Exec pETLSyncDimProducts;
Select Top 1 * From vETLDimProducts Order By ProductID Desc
Select Top 1 * From DimProducts Order By ProductID Desc

-- Test Update to a New Value works as intended.
Update Hint1DB.dbo.Products Set ProductName = 'DDD' Where ProductName = 'CCC' 
Exec pETLSyncDimProducts;
Select Top 2 * From vETLDimProducts Order By ProductID Desc -- ProductID = 79 is once again as it was on the 
Select Top 3 * From DimProducts Order By ProductID Desc -- ProductKey = 79 , ProductID = 79 row
go

-- Test Update to a Previous Value 
Update Hint1DB.dbo.Products Set ProductName = 'CCC' Where ProductName = 'DDD' 
Select Top 2 * From vETLDimProducts Order By ProductID Desc -- ProductID = 80 is once again what it was 
Select Top 2 * From DimProducts Order By ProductID Desc -- on the ProductKey = 81 , ProductID = 80 row
go
--ProductID	ProductName	ProductCategoryID	ProductCategoryName
-->> 80	CCC	9	CatB <<
--ProductKey	ProductID	ProductName	ProductCategoryID	ProductCategoryName	StartDate	EndDate	IsCurrent
--81 >> 80	CCC	9	CatB <<	20200427	20200427	No 

Exec pETLSyncDimProducts; -- ERROR! The Current sproc will not add a new row with the same data
Select Top 2 * From vETLDimProducts Order By ProductID Desc
Select Top 2 * From DimProducts Order By ProductID Desc -- And it looses it IsCurrent status.
go -- We need to fix this!
Update DimProducts Set EndDate = Null, IsCurrent = 'No' Where ProductKey = 82;
Select Top 2 * From vETLDimProducts Order By ProductID Desc
Select Top 2 * From DimProducts Order By ProductID Desc -- OK, that is back to where is was, not let fix the code!


-- We start by adding another column to the view
go
Alter View vETLDimProducts
/* Author: RRoot
** Desc: Extracts and transforms data for DimProducts
** Change Log: When,Who,What
** 2018-01-17,RRoot,Created View.
** 2020-04-27,RRoot,Added IsCurrent column for better merge control
*/
As
  SELECT
    [ProductID] = p.ProductID
   ,[ProductName] = CAST(p.ProductName as nVarchar(100))
   ,[ProductCategoryID] = p.CategoryID
   ,[ProductCategoryName] = CAST(c.CategoryName as nVarchar(100))
   ,[IsCurrent] = 'Yes' -- Added this so we can only join to "IsCurrent" rows in the DW when needed
  FROM [Hint1DB].dbo.Categories as c
  INNER JOIN [Hint1DB].dbo.Products as p
  ON c.CategoryID = p.CategoryID;
go
/* Testing Code:
 Select * From vETLDimProducts;
*/

-- Now we add another condition to the 'On' clause.
go
Alter Procedure pETLSyncDimProducts
/* Author: RRoot
** Desc: Updates data in DimProducts using the vETLDimProducts view
** Change Log: When,Who,What
** 2020-01-01,RRoot,Created Sproc.
** 2020-04-27,RRoot,Added IsCurrent to control Previous Value Issue (Thanks to Aaron L.)
*/
As
Begin
  Declare @ReturnCode int = 0;
  Begin Try
    -- ETL Processing Code --
    Begin Tran;
      Merge Into DimProducts as t
       Using vETLDimProducts as s -- For Merge to work with SCD tables, I need to insert a new row when the following is not true:
        On  t.ProductID = s.ProductID
        And t.ProductName = s.ProductName
        And t.ProductCategoryID = s.ProductCategoryID
        And t.ProductCategoryName = s.ProductCategoryName 
        And t.IsCurrent = s.IsCurrent -- Added to capture row where all but this is a match. This when all is the same, the the is current status then       
       When Not Matched -- At least one column value does not match add a new row:
        Then
         Insert (ProductID, ProductName, ProductCategoryID, ProductCategoryName, StartDate,EndDate,IsCurrent)
          Values (s.ProductID
                ,s.ProductName
                ,s.ProductCategoryID
                ,s.ProductCategoryName
                ,Cast(Convert(nvarchar(100), GetDate(), 112) as int) -- Smart Key can be joined to the DimDate
                ,Null
                ,'Yes')
        When Not Matched By Source -- If there is a row in the target (dim) table that is no longer in the source table
         Then -- indicate that row is no longer current
          Update 
           Set t.EndDate = Cast(Convert(nvarchar(100), GetDate(), 112) as int) -- Smart Key can join to the DimDate
              ,t.IsCurrent = 'No'
              ;
    Commit Tran;
    --( add logging code here )--
    Set @ReturnCode = 1; -- success code
  End Try
  Begin Catch
    If @@TRANCOUNT > 0 Rollback Tran;
    Print Error_Message()
    Set @ReturnCode = -1
  End Catch
  Return @ReturnCode;
End
go

-- Test Update back to Previous Value:
Select Top 2 * From vETLDimProducts Order By ProductID Desc
Select Top 2 * From DimProducts Order By ProductID Desc
go
Exec pETLSyncDimProducts;
Select Top 2 * From Hint1DB.dbo.Products Order By ProductID Desc
Select Top 3 * From DimProducts Order By ProductID Desc
go