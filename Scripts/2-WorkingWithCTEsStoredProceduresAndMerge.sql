
/**************************************************************
 Title: Working with CTEs and Merge
 Desc:This file describes how to design and test advanced
 data retrieval statements using MERGE 
 instead of Intersects and Excepts.
 Change Log: (When,Who,What)
 2022-12-29,RRoot,Created File
**************************************************************/

-- To start, let' create a two tables, whose data can be merged 
-- and a stored procedure that will help us manange the demo. 
Use TempDB;
Go
If (object_id('pResetDemo') is not null) Drop Procedure pResetDemo;
Go
Create Or Alter Procedure pResetDemo
AS
	If (object_id('Customers') is not null) Drop Table Customers;
	Create
	Table Customers 
	(CustomerID int Primary Key Identity, CustomerName nVarchar(100), CustomerEmail nVarchar(100) Unique);

	If (object_id('DimCustomers') is not null) Drop Table DimCustomers;
	Create
	Table DimCustomers 
	(CustomerID int Primary Key, CustomerName nVarchar(100), CustomerEmail nVarchar(100));

	-- Now let's add some data to the first table. Let's consider this the "SOURCE" table
	Insert into Customers(CustomerName, CustomerEmail)
		Values ('Bob Smith' , 'BSmith@DemoCo.com')
			    , ('Sue Jones' , 'SJones@DemoCo.com');
	-- Now, let's compare the differences
	Select CustomerID, CustomerName, CustomerEmail From Customers;
	Select CustomerID, CustomerName, CustomerEmail From DimCustomers;
Go

Execute pResetDemo;
Go

-- Now, let's compare the differences
	Select CustomerID, CustomerName, CustomerEmail From Customers
	Except
	Select CustomerID, CustomerName, CustomerEmail From DimCustomers;
	Go


--( Using A Common Table Expression [CTE] )--
--'********************************************************************************************'
-- We can trasfer data to the dimension table by using a INSERT statement combined with EXCEPT. 
With ChangedCustomers -- This creates a CTE we can refer back to in our code.
As
(	-- Find the IDs that a new.
	Select CustomerID From Customers
	Except
	Select CustomerID From DimCustomers
)
Insert Into DimCustomers(CustomerID, CustomerName, CustomerEmail)
	Select CustomerID, CustomerName, CustomerEmail From Customers
	Where CustomerID in (Select CustomerID from ChangedCustomers);

-- And, compare the differences
	Select CustomerID, CustomerName, CustomerEmail From Customers;
	Select CustomerID, CustomerName, CustomerEmail From DimCustomers;
	Go

-- Ok, so that works with an Insert but what about updates? --
Update Customers
	Set CustomerName = 'RobertSmith'
	  , CustomerEmail = 'RSmith@DemoCo.com'
	Where CustomerID = 1

-- Now, let's compare the differences
	Select CustomerID, CustomerName, CustomerEmail From Customers;
	Select CustomerID, CustomerName, CustomerEmail From DimCustomers;
	Go


-- Now, let's force an update to the tables where there are differences in the rows, using Correlated Subqueries
With ChangedCustomers 
As
(	-- Find rows that have different data
	Select CustomerID, CustomerName, CustomerEmail From Customers
	Except
	Select CustomerID, CustomerName, CustomerEmail From DimCustomers
)
Update DimCustomers
	Set CustomerName = (Select CustomerName From ChangedCustomers Where ChangedCustomers.CustomerID = DimCustomers.CustomerID) -- This query must run for each row being updated!
	  , CustomerEmail = (Select CustomerEmail From ChangedCustomers Where ChangedCustomers.CustomerID = DimCustomers.CustomerID) -- So does this one!
	Where CustomerId In (Select CustomerId From ChangedCustomers );

-- Now, let's compare the differences
	Select CustomerID, CustomerName, CustomerEmail From Customers;
	Select CustomerID, CustomerName, CustomerEmail From DimCustomers;
	Go

-- Now that we have the Insert and Update working, lets create a code for Deletes --
  Delete
	From Customers
	Where CustomerID = 2;

-- Now, let's compare the differences
	Select CustomerID, CustomerName, CustomerEmail From Customers;
	Select CustomerID, CustomerName, CustomerEmail From DimCustomers;
	Go

With ChangedCustomers 
As
( -- Note that I had to change the order of the tables to make the delete work!
	Select CustomerID, CustomerName, CustomerEmail From DimCustomers
	Except
	Select CustomerID, CustomerName, CustomerEmail From Customers
)
Delete 
	From DimCustomers
	Where CustomerID In (Select CustomerID from ChangedCustomers)

-- Now, let's compare the differences
	Select CustomerID, CustomerName, CustomerEmail From Customers;
	Select CustomerID, CustomerName, CustomerEmail From DimCustomers;
	Go

--( USING SIMPLE TRANSACTION STORED PROCEDURES )--
--'********************************************************************************************'--

/*********** Typical Template *********** ***
Create Or Alter Procedure <NAME OF SPROC>
(<Parmenter Listing>)
/* 
 Dev:
 Desc:
 Changelog: (Who, What, When)	
*/
AS 
BEGIN  
  Declare @ReturnCode int;
  --( add validation code here )--
  Begin Try
    --( add processing code here )--
    Begin Tran;
    Commit Tran;
    --( add logging code here )--
    Set @ReturnCode = 1 -- success code
  End Try
  Begin Catch
	  --( add error handling and logging code here )--
    If @@TRANCOUNT > 0 Rollback Tran;
    Set @ReturnCode = -1; -- fail code
  End Catch
  Return @ReturnCode;
END;
*******************************************/

-- Let's start by resetting the demo tables
Execute pResetDemo;
Go

-- Now lets create some traditional ETL stored procedures
Create Or Alter Procedure pETLInsDimCustomers
/* 
 Dev: RRoot
 Desc: Inserts data into DimCustomers
 Changelog: (Who, What, When)	
*/
AS 
BEGIN 
  Declare @ReturnCode int
  --( add validation code here )--
  Begin Try
    Begin Tran; --< NOTE: You must include a ; before starting the CTE!
      With ChangedCustomers 
      As
      (
	      Select CustomerID From Customers
	      Except
	      Select CustomerID From DimCustomers
      )
      Insert Into DimCustomers(CustomerID, CustomerName, CustomerEmail)
	      Select CustomerID, CustomerName, CustomerEmail From Customers
	      Where CustomerID in (Select CustomerID from ChangedCustomers);
    Commit Tran;
    --( add logging code here )--
    Set @ReturnCode = 1; -- success code
  End Try
  Begin Catch
    --( add error handling code here )--
    If @@TRANCOUNT > 0 Rollback Tran
    Set @ReturnCode = -1 -- fail code
  End Catch
  Return @ReturnCode
END
;-- Exec pETLInsDimCustomers
Go

Create Or Alter Procedure pETLUpdDimCustomers
/* 
 Dev: RRoot
 Desc: Updates data in DimCustomers
 Changelog: (Who, What, When)	
*/
AS 
BEGIN 
  Declare @ReturnCode int
  --( add validation code here )--
  Begin Try
    Begin Tran;
    With ChangedCustomers 
    As
    (
    	Select CustomerID, CustomerName, CustomerEmail From Customers
    	Except
    	Select CustomerID, CustomerName, CustomerEmail From DimCustomers
    )
    Update DimCustomers
    	Set CustomerName = (Select CustomerName From ChangedCustomers Where ChangedCustomers.CustomerID = DimCustomers.CustomerID) -- This query must run for each row being updated!
    	     ,CustomerEmail = (Select CustomerEmail From ChangedCustomers Where ChangedCustomers.CustomerID = DimCustomers.CustomerID) -- So does this one!
    	Where CustomerId In (Select CustomerId From ChangedCustomers ) ;
        Set @ReturnCode = 1 -- success code
    Commit Tran;
    --( add logging code here )--
    Set @ReturnCode = 1; -- success code
  End Try
  Begin Catch
    --( add error handling code here )--
    Set @ReturnCode = -1 -- fail code
  End Catch
  Return @ReturnCode
END;
GO

Create Or Alter Procedure pETLDelDimCustomers
/* 
  Dev: RRoot
  Desc: Deletes data from DimCustomers
  Changelog: (Who, What, When)	
 */
AS 
BEGIN 
  Declare @ReturnCode int
	--( add validation code here )--
  Begin Try
    Begin Tran;
		  With ChangedCustomers 
		  As
		  ( -- Note that I had to change the order of the tables to make the delete work!
		  	Select CustomerID, CustomerName, CustomerEmail From DimCustomers
		  	Except
		  	Select CustomerID, CustomerName, CustomerEmail From Customers
		  )
		  Delete 
		  	From DimCustomers
		  	Where CustomerID In (Select CustomerID from ChangedCustomers)
	    Set @ReturnCode = 1 -- success code
    Commit Tran;
    --( add logging code here )--
    Set @ReturnCode = 1; -- success code
  End Try
	Begin Catch
	  --( add error handling code here )--
    If @@TRANCOUNT > 0 Rollback Tran;
	  Set @ReturnCode = -1; -- fail code
	End Catch
	Return @ReturnCode
END;
Go

-- Now lets combine them into a Controlling ETL processing stored procedure
Create Or Alter Procedure pETLDimCustomers
/* 
 Dev: RRoot
 Desc: Inserts, Updates, and Deletes data in DimCustomers
 Changelog: (Who, What, When)	
*/
AS 
BEGIN 
  Declare @ReturnCode int
  --( add validation code here )--
  Begin Try
	  Execute pETLInsDimCustomers;
	  Execute pETLUpdDimCustomers;
	  Execute pETLDelDimCustomers;
    Set @ReturnCode = 1 -- success code
  End Try
  Begin Catch
    --( add error handling code here )--
    Set @ReturnCode = -1 -- fail code
    End Catch
  Return @ReturnCode
END;
Go

-- Let's start by resetting the demo tables
Execute pResetDemo;
Go

Execute pETLDimCustomers;
-- Now, let's compare the differences
	Select CustomerID, CustomerName, CustomerEmail From Customers;
	Select CustomerID, CustomerName, CustomerEmail From DimCustomers;
Go

-- Ok, so that works with an Insert but what about updates? --
Update Customers
	Set CustomerName = 'RobertSmith'
	     ,CustomerEmail = 'RSmith@DemoCo.com'
	Where CustomerID = 1;
Go

Execute pETLDimCustomers;
-- Now, let's compare the differences
	Select CustomerID, CustomerName, CustomerEmail From Customers;
	Select CustomerID, CustomerName, CustomerEmail From DimCustomers;
Go

-- Now that we have the Insert and Update working, lets create a code for Deletes --
  Delete
	From Customers
	Where CustomerID = 2;

Execute pETLDimCustomers;
-- Now, let's compare the differences
	Select CustomerID, CustomerName, CustomerEmail From Customers;
	Select CustomerID, CustomerName, CustomerEmail From DimCustomers;
Go

--( Using Merge, for more efficent ETL processing )--
--*****************************************************************************************-- 
-- Let's start by resetting the demo tables
Execute pResetDemo;
Go
	Merge Into DimCustomers as TargetTable
	Using Customers as SourceTable
		ON TargetTable.CustomerID = SourceTable.CustomerID
		When Not Matched 
			Then -- The ID in the Source is not found the the Target
				INSERT 
				VALUES (SourceTable.CustomerID, SourceTable.CustomerName, SourceTable.CustomerEmail )
	; -- The merge statement demands a semicolon at the end!

-- Now, let's compare the differences
	Select CustomerID, CustomerName, CustomerEmail From Customers;
	Select CustomerID, CustomerName, CustomerEmail From DimCustomers;
Go


-- Ok, so that works with an Insert but what about updates? --
Update Customers
	Set CustomerName = 'RobertSmith'
	     ,CustomerEmail = 'RSmith@DemoCo.com'
	Where CustomerID = 1;
Go
	Merge Into DimCustomers as TargetTable
	Using Customers as SourceTable
		ON TargetTable.CustomerID = SourceTable.CustomerID
		When Not Matched 
			Then -- The ID in the Source is not found the the Target
				INSERT 
				VALUES ( SourceTable.CustomerID, SourceTable.CustomerName, SourceTable.CustomerEmail )
		When Matched -- When the IDs match for the row currently being looked 
		AND ( SourceTable.CustomerName <> TargetTable.CustomerName -- but the Names 
			OR SourceTable.CustomerEmail <> TargetTable.CustomerEmail ) -- or Email do not match...
			Then 
				UPDATE -- It know your target, so you dont specify the DimCustomers
				SET TargetTable.CustomerName = SourceTable.CustomerName
					, TargetTable.CustomerEmail = SourceTable.CustomerEmail
	; -- The merge statement demands a semicolon at the end!
Go

-- Now, let's compare the differences
	Select CustomerID, CustomerName, CustomerEmail From Customers;
	Select CustomerID, CustomerName, CustomerEmail From DimCustomers;
Go

-- Now that we have the Insert and Update working, lets create a code for Deletes --
  Delete
	From Customers
	Where CustomerID = 2;
Go
	Merge Into DimCustomers as TargetTable
	Using Customers as SourceTable
		ON TargetTable.CustomerID = SourceTable.CustomerID
		When Not Matched 
			Then -- The ID in the Source is not found the the Target
				INSERT 
				VALUES ( SourceTable.CustomerID, SourceTable.CustomerName, SourceTable.CustomerEmail )
		When Matched -- When the IDs match for the row currently being looked 
		AND ( SourceTable.CustomerName <> TargetTable.CustomerName -- but the Names 
			OR SourceTable.CustomerEmail <> TargetTable.CustomerEmail ) -- or Email do not match...
			Then 
				UPDATE -- It know your target, so you dont specify the DimCustomers
				SET TargetTable.CustomerName = SourceTable.CustomerName
					, TargetTable.CustomerEmail = SourceTable.CustomerEmail
		When Not Matched By Source 
			Then -- The CustomerID is in the Target table, but not the source table
				DELETE
	; -- The merge statement demands a semicolon at the end!

-- Now, let's compare the differences
	Select CustomerID, CustomerName, CustomerEmail From Customers;
	Select CustomerID, CustomerName, CustomerEmail From DimCustomers;
Go


-- Now I would place the code into My stored procedure. --
Create Or Alter Procedure pETLDimCustomers
/* 
 Dev: RRoot
 Desc: Inserts, Updates, and Deletes data in DimCustomers
 Changelog: (Who, What, When)
RRoot,Changed code to use Merge, 1/1/2020	
*/
AS 
BEGIN 
  Declare @ReturnCode int;
	--( add validation code here )--
  Begin Try
    Begin Tran;
		  Merge Into DimCustomers as TargetTable
		  Using Customers as SourceTable
		  	ON TargetTable.CustomerID = SourceTable.CustomerID
		  	When Not Matched 
		  		Then -- The ID in the Source is not found the the Target
		  			INSERT 
		  			VALUES ( SourceTable.CustomerID, SourceTable.CustomerName, SourceTable.CustomerEmail )
		  	When Matched -- When the IDs match for the row currently being looked 
		  	AND ( SourceTable.CustomerName <> TargetTable.CustomerName -- but the Names 
		  		OR SourceTable.CustomerEmail <> TargetTable.CustomerEmail ) -- or Email do not match...
		  		Then 
		  			UPDATE -- It know your target, so you dont specify the DimCustomers
		  			SET TargetTable.CustomerName = SourceTable.CustomerName
		  			  , TargetTable.CustomerEmail = SourceTable.CustomerEmail
		  	When Not Matched By Source 
		  		Then -- The CustomerID is in the Target table, but not the source table
		  			DELETE
		  ; -- The merge statement demands a semicolon at the end!
    Commit Tran;
    --( add logging code here )--
    Set @ReturnCode = 1; -- success code
  End Try
	Begin Catch
	  --( add error handling code here )--
    If @@Trancount > 0 RollBack Tran;
	  Set @ReturnCode = -1; -- fail code
	End Catch
	Return @ReturnCode;
END;

-- Let's start by resetting the demo tables
Execute pResetDemo;
Go

-- Now, let's compare the differences
	Select CustomerID, CustomerName, CustomerEmail From Customers;
	Select CustomerID, CustomerName, CustomerEmail From DimCustomers;
	Go

  
-- Executing the stored procedure will perform the ETL process
EXEC pETLDimCustomers
Go
-- Now, let's compare the differences
	Select CustomerID, CustomerName, CustomerEmail From Customers;
	Select CustomerID, CustomerName, CustomerEmail From DimCustomers;
	Go

-- Ok, so that works with an Insert but what about updates? --
Update Customers
	Set CustomerName = 'RobertSmith'
	     ,CustomerEmail = 'RSmith@DemoCo.com'
	Where CustomerID = 1
Go

Execute pETLDimCustomers;
-- Now, let's compare the differences
	Select CustomerID, CustomerName, CustomerEmail From Customers;
	Select CustomerID, CustomerName, CustomerEmail From DimCustomers;
Go

-- Now that we have the Insert and Update working, lets create a code for Deletes --
  Delete
	From Customers
	Where CustomerID = 2;

Execute pETLDimCustomers;
-- Now, let's compare the differences
	Select CustomerID, CustomerName, CustomerEmail From Customers;
	Select CustomerID, CustomerName, CustomerEmail From DimCustomers;
Go


-- Simple Transformations can also be applied if you wish...
-- Now, let's add some simple transformation code the the stored procedure...
ALTER 
PROCEDURE pETLDimCustomers
  /* 
  Dev: RRoot
  Desc: Inserts, Updates, and Deletes data in DimCustomers
  Changelog: (Who, What, When)
  	RRoot,Changed code to use Merge, 1/1/2020
	RRoot,Added additional Transformation code, 1/2/2020		
 */
  AS 
  BEGIN 
    Declare @ReturnCode int;
	--( add validation code here )--
    Begin Try
      Begin Tran;
		    Merge Into DimCustomers as TargetTable
		    Using Customers as SourceTable
		    	ON TargetTable.CustomerID = SourceTable.CustomerID
		    	When Not Matched 
		    		Then -- The ID in the Source is not found the the Target
		    			INSERT 
		    			VALUES ( Cast(SourceTable.CustomerID as SmallInt), Right(SourceTable.CustomerName, 5), ISNull(SourceTable.CustomerEmail , 'NA') )
		    	When Matched -- When the IDs match for the row currently being looked 
		    	AND (  SourceTable.CustomerName <> TargetTable.CustomerName -- but the Names 
		    		OR SourceTable.CustomerEmail <> TargetTable.CustomerEmail ) -- or Email do not match...
		    		Then 
		    			UPDATE -- It know your target, so you dont specify the DimCustomers
		    			SET TargetTable.CustomerName = Right(SourceTable.CustomerName, 5 )
		    			  , TargetTable.CustomerEmail = ISNull( SourceTable.CustomerEmail, 'NA')
		    	When Not Matched By Source 
		    		Then -- The CustomerID is in the Target table, but not the source table
		    			DELETE
		    ; -- The merge statement demands a semicolon at the end!
    Commit Tran;
    --( add logging code here )--
    Set @ReturnCode = 1; -- success code
	  End Try
	  Begin Catch
	    --( add error handling code here )--
      If @@Trancount > 0 Rollback Tran;
	    Set @ReturnCode = -1 -- fail code
	  End Catch
	Return @ReturnCode
  END;
Go

-- To demo this, lets reset the data to the Source table, Customers once more
Insert into Customers(CustomerName, CustomerEmail)
	Values ('Sue Jones', Null);
Go
Update Customers
	Set CustomerName = 'Robert Smith'
	     ,CustomerEmail = 'BSmith@DemoCo.com'
	Where CustomerID = 1
Go
-- Now, let's compare the differences
	Select CustomerID, CustomerName, CustomerEmail From Customers;
	Select CustomerID, CustomerName, CustomerEmail From DimCustomers;
Go

-- Executing the stored procedure will perform the ETL process
EXEC pETLDimCustomers

-- Now, let's compare the differences
	Select CustomerID, CustomerName, CustomerEmail From Customers;
	Select CustomerID, CustomerName, CustomerEmail From DimCustomers;
	Go

-- You could even use a custom functions!
Create Or Alter Function dbo.fETLCustomerName(@CustomerName nVarchar(100))
Returns nVarchar(100)
AS 
BEGIN
  Return ( -- Change data to  "Last Name - first and First Name - last" format
	 Select 
	 Substring(@CustomerName, patindex('% %', @CustomerName), 100 )   
	 + ','
	 + Substring(@CustomerName, 0 , patindex('% %', @CustomerName)) 
 ) 
END
Go

-- Now you can use that code with your Merge statement to perform more complex merging
ALTER
 PROCEDURE pETLDimCustomers
  /* 
  Dev: RRoot
  Desc: Inserts, Updates, and Deletes data in DimCustomers
  Changelog: (Who, What, When)
    RRoot,Changed code to use Merge, 1/1/2020
	RRoot,Added additional Transformation code, 1/2/2020
	RRoot,Added the use of dbo.fETLCustomerName for Transformations, 1/3/2020	
 */
AS 
BEGIN 
  Declare @ReturnCode int;
  --( add validation code here )--
  Begin Try
    Begin Tran;
	    Merge Into DimCustomers as TargetTable
	    Using Customers as SourceTable
	    	ON TargetTable.CustomerID = SourceTable.CustomerID
	    	When Not Matched 
	    		Then -- The ID in the Source is not found the the Target
	    			INSERT 
	    			VALUES ( Cast(SourceTable.CustomerID as SmallInt), dbo.fETLCustomerName(SourceTable.CustomerName), ISNull(SourceTable.CustomerEmail , 'NA') )
	    	When Matched -- When the IDs match for the row currently being looked 
	    	AND (  SourceTable.CustomerName <> TargetTable.CustomerName -- but the Names 
	    		OR SourceTable.CustomerEmail <> TargetTable.CustomerEmail ) -- or Email do not match...
	    		Then 
	    			UPDATE -- It know your target, so you dont specify the DimCustomers
	    			SET TargetTable.CustomerName = dbo.fETLCustomerName(SourceTable.CustomerName)
	    			  , TargetTable.CustomerEmail = ISNull( SourceTable.CustomerEmail, 'NA')
	    	When Not Matched By Source 
	    		Then -- The CustomerID is in the Target table, but not the source table
	    			DELETE
	    ; -- The merge statement demands a semicolon at the end!
    Commit Tran;
    --( add logging code here )--
    Set @ReturnCode = 1; -- success code
  End Try
  Begin Catch
    --( add error handling code here )--
    If @@Trancount > 0 Rollback Tran;
    Set @ReturnCode = -1; -- fail code
  End Catch
  Return @ReturnCode;
END;
Go

-- Test that it works...
Execute pResetDemo;

-- Now, let's compare the differences
	Select CustomerID, CustomerName, CustomerEmail From Customers;
	Select CustomerID, CustomerName, CustomerEmail From DimCustomers;
Go

-- Executing the stored procedure will perform the ETL process
EXEC pETLDimCustomers

-- Now, let's compare the differences
	Select CustomerID, CustomerName, CustomerEmail From Customers;
	Select CustomerID, CustomerName, CustomerEmail From DimCustomers;
	Go


-- Ok, so that works with an Insert but what about updates? --
Update Customers
	Set CustomerName = 'Robert Smith'
	     ,CustomerEmail = 'RSmith@DemoCo.com'
	Where CustomerID = 1
Delete
	From Customers
	Where CustomerID = 2;

-- Now, let's compare the differences
	Select CustomerID, CustomerName, CustomerEmail From Customers;
	Select CustomerID, CustomerName, CustomerEmail From DimCustomers;
Go

-- Executing the stored procedure will perform the ETL process
EXEC pETLDimCustomers

-- Now, let's compare the differences
	Select CustomerID, CustomerName, CustomerEmail From Customers;
	Select CustomerID, CustomerName, CustomerEmail From DimCustomers;
	Go