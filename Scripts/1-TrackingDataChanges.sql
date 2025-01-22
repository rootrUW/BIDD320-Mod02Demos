--****************** SQL Programming *********************--
-- This file highlights a history of commonly used
-- ways to track changes to data.
--**********************************************************--
Use Master
If Exists (Select * from Sysdatabases Where Name = 'TrackingChangesDB')
	Begin 
	ALTER DATABASE [TrackingChangesDB] SET SINGLE_USER WITH ROLLBACK IMMEDIATE
	DROP DATABASE [TrackingChangesDB]
	End
Go
Create Database TrackingChangesDB
Go
Use TrackingChangesDB
Go

'*** Tracking Changes with Triggers ***'
-----------------------------------------------------------------------------------------------------------------------
-- Perhaps the oldest way of tracking tables changes is by the use of Triggers

-- First lets create a table for the demo
Create -- Drop
Table Customers
( CustomerId int, CustomerName varchar(50))
Go

-- Now let's test out some functions that are typically added to the data capture
Select GetDate(), Suser_sName(), User
Go

-- This simple trigger shows how the Inserted and Deleted pseudo tables work.
Create -- Alter
Trigger trCustomers
	On Customers
	For Insert, Update, Delete
AS
  Select CustomerId as [InsCustomerId], CustomerName, GetDate(), sUser_sName(), User from Inserted
  Select CustomerId as [delCustomerId], CustomerName, GetDate(), sUser_sName(), User from Deleted
Go

-- Now let's try some transactional statements.
Insert into Customers (CustomerId, CustomerName) 
	Values(1, 'Bob Smith')
Select * from Customers
Go

Update Customers 
	Set CustomerName = 'Robert Smith' 
	Where CustomerId = 1
Select * from Customers
Go

Delete From Customers 
	Where CustomerId = 1
Select * from Customers
Go

-- One way to capture data changes it to create a tracking table and use the trigger to automatically
-- add data to it when changes occure to the table you want to monitor. 
Create -- Drop
Table CustomersChanges
( CustomerId int, CustomerName varchar(50), DateAdded datetime, TransactionType Char(1))
Go

-- Now create a Trigger that uses the Tracking table.
Alter
Trigger trCustomers
	On Customers
	For Insert, Update, Delete
AS
-- Check for Update
If (((Select Count(*) from Inserted) > 0) and ((Select Count(*) from Deleted) > 0))
	Begin
		Insert into CustomersChanges(CustomerId, CustomerName, DateAdded, TransactionType)
		Select CustomerId, CustomerName, GetDate(), 'u' from Inserted
	End
-- Check for Insert	
Else If ((Select Count(*) from Inserted) > 0)	
	Begin
		Insert into CustomersChanges(CustomerId, CustomerName, DateAdded, TransactionType)
		Select CustomerId, CustomerName, GetDate(), 'i' from Inserted
	End
-- Check for Delete
Else If ((Select Count(*) from Deleted) > 0)	
	Begin
		Insert into CustomersChanges(CustomerId, CustomerName, DateAdded, TransactionType)
		Select CustomerId, CustomerName, GetDate(), 'd' from Deleted
	End	
Go

-- let's try those transactional statements once again
Insert into Customers (CustomerId, CustomerName)  
	Values(1, 'Bob Smith')
Select * from CustomersChanges
Go

Update Customers 
	Set CustomerName = 'Robert Smith' 
	Where CustomerId = 1
Select * from CustomersChanges
Go

Delete From Customers 
	Where CustomerId = 1
Select * from CustomersChanges
Go

-- Now let's see the final results
Select * from Customers
Select * from CustomersChanges


-- Reset Demo --
Drop Table Customers
	-- Drop Trigger trCustomers <-- This is Not needed, since dropping the table it's bound to drops the trigger
Drop Table CustomersChanges
Go

'*** Tracking with a RowVersion column ***'
-----------------------------------------------------------------------------------------------------------------------
-- The RowVersion (or Timestamp) data type automatically increments whenever a change happen on a table row.
-- These VALUES are always automatic and cannot be added manually.
-- They are also always unique thoughout a particular database.
-- This data type is equivelant to the binary(8) data type which can be use when you want to 
-- manually copy this data to another table

-- Recreate the customers table, but this time with a TRACKING COLUMN...
Create -- Drop
Table Customers
( CustomerId int, CustomerName varchar(50), ChangeTracker RowVersion)
Go
 
 -- Let's add a value
Insert into Customers(CustomerId, CustomerName) 
	Values(1, 'Bob Smith')
Select * from Customers
Go
 
-- You can still capture data changes using a TRACKING TABLE and repeatly checking 
-- for changes in the RowVersion column. Note the the binary(8) data type 
Create -- Drop
Table CustomersChanges
( CustomerId int, CustomerName varchar(50), ChangeTracker binary(8),  DateCopied datetime Default GetDate())
Go

-- Now they are out of sync
Select * from Customers
Select * From CustomersChanges

-- Isolate the differences.
Select CustomerId, CustomerName, Cast(ChangeTracker as binary(8)) from Customers
Except -- EXCEPT returns any distinct values from the "First" query that are not found in the "Second" query.
Select CustomerId, CustomerName, ChangeTracker from CustomersChanges 


-- Syncronize tables for new Inserts or Updates, but deletes have to be done a different way...
Insert into CustomersChanges(CustomerId, CustomerName,ChangeTracker)
	Select CustomerId, CustomerName, Cast(ChangeTracker as binary(8)) from Customers 
	Except -- EXCEPT returns any distinct values from the "First" query that are not also found on the "Second" query.
	Select CustomerId, CustomerName, ChangeTracker from CustomersChanges
	
-- Now they are syncronized
Select * from Customers
Select * From CustomersChanges	
Go

/* NOTE:
** Normally you would place this insert code a Store Procedure and force developers to use it
** instead of inserting directly into a tables. We could use conditional logic in the Store Procedure 
** to tell the difference between and Insert or an Update by adding and "Insert Flag" column to 
** the Customers table. However, for simplicty we will skip that in this demo. 
*/ 

-- Now they are out of sync
Insert into Customers(CustomerId, CustomerName) 
	Values(2, 'Sue Jones')
Select * from Customers
Select * From CustomersChanges
Go

-- Syncronize tables for new Inserts or Updates...
Insert into CustomersChanges(CustomerId, CustomerName,ChangeTracker)
	Select CustomerId, CustomerName, Cast(ChangeTracker as binary(8))from Customers
	Except
	Select CustomerId, CustomerName, ChangeTracker from CustomersChanges
Go

-- Now they are syncronized again
Select * from Customers
Select * From CustomersChanges	
Go

-- Now they are out of sync, yet again.
Update Customers
	Set CustomerName = 'Robert Smith'
	Where CustomerId = 1
Select * from Customers
Select * From CustomersChanges
Go

-- Syncronize tables for new Inserts or Updates...
Insert into CustomersChanges(CustomerId, CustomerName,ChangeTracker )
	Select CustomerId, CustomerName, Cast(ChangeTracker as binary(8)) from Customers
	Except
	Select CustomerId, CustomerName, ChangeTracker from CustomersChanges
Go

-- Now they are syncronized again
Select * from Customers
Select * From CustomersChanges	 
Go

-- Now they are out of sync, once more.
Delete From Customers
	Where CustomerId = 1
Select * from Customers
Select * From CustomersChanges
Go

-- Since the Except option always check the first table against the second we do not see the deletion.
Select CustomerId, CustomerName, Cast(ChangeTracker as binary(8)) from Customers
Except
Select CustomerId, CustomerName, ChangeTracker from CustomersChanges


-- However Changing the order of tables will allow us to do so!
Select CustomerId, CustomerName, ChangeTracker from CustomersChanges
Except
Select CustomerId, CustomerName, Cast(ChangeTracker as binary(8)) from Customers


-- Syncronize tables for new Deletes...
Delete From CustomersChanges 
	Where CustomerId In (
			Select CustomerId from CustomersChanges
			Except
			Select CustomerId from Customers
		)
Select * from Customers
Select * From CustomersChanges	 
Go


-- The problem with the our Except option is that it does not 
-- distinguish between the Insert and Update transactions.
-- However, when we do a similar thing using a set of Join statements we can tell the difference.

Update Customers
	Set CustomerName = 'Sue Thomson' 
	Where CustomerId = 2
Go
--Check for Inserts
Select Customers.CustomerId, Customers.CustomerName, Cast(Customers.ChangeTracker as binary(8)) 
From Customers Join CustomersChanges
	On Customers.CustomerId != CustomersChanges.CustomerId -- ID is different
	AND Customers.CustomerName != CustomersChanges.CustomerName -- Name is different
	AND Customers.ChangeTracker != CustomersChanges.ChangeTracker -- Row version is different
-- (0 row(s) affected)	

-- Check for Update on [CustomerId] column
Select Customers.CustomerId, Customers.CustomerName, Cast(Customers.ChangeTracker as binary(8)) 
From Customers Join CustomersChanges
	On Customers.CustomerId != CustomersChanges.CustomerId -- ID is different
	AND Customers.CustomerName = CustomersChanges.CustomerName -- Name is the same
	AND Customers.ChangeTracker != CustomersChanges.ChangeTracker -- Row version is different
-- (0 row(s) affected)	

-- Check for Update on [CustomerName] column
Select Customers.CustomerId, Customers.CustomerName, Cast(Customers.ChangeTracker as binary(8)) 
From Customers Join CustomersChanges
	On Customers.CustomerId = CustomersChanges.CustomerId -- IDs is the same
	AND Customers.CustomerName != CustomersChanges.CustomerName -- Name is different
	AND Customers.ChangeTracker != CustomersChanges.ChangeTracker -- Row version is different

/* NOTE:
** Creating a Stored Procedure, placing these joins within it, and using them to determine
** which type of transaction would would need to sync the tacking table would be a better tracking system. 
** Or, we could just use a Trigger :-0
** Its a shame that Triggers are "Bad". ;-) 
*/

-- Reset Demo --
Drop Table Customers
Drop Table CustomersChanges
Go

'*** Tracking with a Flag column ***'
-----------------------------------------------------------------------------------------------------------------------
-- Another common method of Tracking changes is by including a flag in the original table:
-- Typical flags are (i)Inserted, (u)Updated, (d)Deleted, (null)Unchanged
Use TrackingChangesDB
Go
Create -- Drop
Table Customers
( CustomerId int, CustomerName varchar(50), RowStatus Char(1) check(RowStatus in ('i','u','d')) )
Go

-- Now let's try some transactional statements.
Insert into Customers (CustomerId, CustomerName, RowStatus) 
	Values(1, 'Bob Smith', 'i')
Select * from Customers
Go

-- Now when we import data to a replica table we just reset the flags to null 
Create -- Drop
Table CustomersChangeTracker
( CustomerId int, CustomerName varchar(50), DateAdded datetime, TransactionType Char(1))
Go

-- Copy the data from the Customers table
Create -- Drop
Proc pSyncByFlag
as
-- Get the changed data...
Insert into CustomersChangeTracker (CustomerId, CustomerName, DateAdded, TransactionType) 
	Select CustomerId, CustomerName, GetDate(), RowStatus 
	From Customers
	Where RowStatus is not null
--... and reset the flags
Update Customers Set RowStatus = null
-- We will, display the tables contents for testing
Select * from Customers
Select * from CustomersChangeTracker
Go

-- Ok, let sync the tables
Exec pSyncByFlag
Go

-- Now, let's do somemore transactional statements.
Insert into Customers (CustomerId, CustomerName, RowStatus) 
	Values(2, 'Sue Jones', 'i')
Select * from Customers	
Go	

Update Customers 
	Set CustomerName = 'Robert Smith' 
		, RowStatus = 'u'
	Where CustomerId = 1
Select * from Customers	
Go	

Exec pSyncByFlag
Go	
	
-- When we need to Delete From Customers, we use an update instead!
Update Customers 
	Set RowStatus = 'd'
	Where CustomerId = 2
Select * from Customers
Go

Exec pSyncByFlag
Go	


-- Reset Demo --
Drop Table Customers
Drop Table CustomersChangeTracker
Go

/* NOTE:
** When we use a Tracking table, the process is sometimes referred to as a 
** Type 4 SCD(Slow Changing Dimension). This is a term used with Data Warehousing.
** We, will take a look at SCD Types 1, 2, and 3 next.
*/

'*** Tracking with Slow Changing Dimension columns ***'
-----------------------------------------------------------------------------------------------------------------------
Use TrackingChangesDB
Go
-- Type 1 (aka: "No One Really Cares")
-- In these SCD, you just overwrite the existing data and forget it!
Create -- Drop
Table Customers
( CustomerId int, CustomerName varchar(50))
Go

-- Add a new row
Insert into Customers (CustomerId, CustomerName ) 
	Values(1, 'Bob Smith')
Select * from Customers
Go
-- Change an Existing row
Update Customers 
	Set CustomerName = 'Rob Smith' 
	Where CustomerId = 1
Select * from Customers

-- Change an Existing row
Update Customers 
	Set CustomerName = 'Robert Smith' 
	Where CustomerId = 1
Select * from Customers


-- Type 3 (aka: "What was it last time?")
-- This method tracks previous value using separate columns
-- In These SCD, you just overwrite the existing data and forget it!
Create -- Drop
Table Customers
( CustomerId int, CustomerName varchar(50), OldCustomerName varchar(50))
Go

-- Add a new row
Insert into Customers (CustomerId, CustomerName, OldCustomerName ) 
	Values(1, 'Bob Smith', Null)
Select * from Customers
Go

-- Change an Existing row
Update Customers 
	Set CustomerName = 'Rob Smith'
	, OldCustomerName = 'Bob Smith'	 
	Where CustomerId = 1
Select * from Customers


-- Change an Existing row
Update Customers 
	Set CustomerName = 'Robert Smith'
	, OldCustomerName = 'Rob Smith'	 
	Where CustomerId = 1
Select * from Customers

-- Type 2 (aka: "I want them all!")
-- This popular method tracks an infinite number of versions by just adding 
-- a Version column to the table and forcing people to do only inserts
-- instead of deletes.

Create -- Drop
Table Customers (
  CustomerId int
, CustomerName varchar(50)
, VersionId int
Primary Key (CustomerId, VersionId ) 
)
Go

-- Add a new row
Insert into Customers (CustomerId, CustomerName, VersionId ) 
	Values(1, 'Bob Smith', 0)
Select * from Customers
Go

-- Change an Existing row by Adding a new one with a new VersionId
Insert into Customers (CustomerId, CustomerName, VersionId ) 
	Values(1, 'Rob Smith', 1)
Select * from Customers
Go

-- Change an Existing row by Adding a new one with a new VersionId
Insert into Customers (CustomerId, CustomerName, VersionId ) 
	Values(1, 'Robert Smith', 2)
Select * from Customers
Go
-- Type 2 part b (aka: "I still want more!")
-- You can enhance your version control, and provide more accurtate 
-- reporting against the table, by adding a way to track when the 
-- version was in effect.

Create -- Drop
Table Customers (
  CustomerId int
, CustomerName varchar(50)
, VersionId int
, StartDate DateTime
Primary Key (CustomerId, VersionId ) 
)
Go

-- Add a new row
Insert into Customers (CustomerId, CustomerName, VersionId, StartDate) 
	Values(1, 'Bob Smith', 0, GetDate() )
Select * from Customers
Go

Insert into Customers (CustomerId, CustomerName, VersionId, StartDate) 
	Values(1, 'Rob Smith', 1, GetDate() )
Select * from Customers
Go

-- Change an Existing row by Adding a new one with a new VersionId and the new date
Insert into Customers (CustomerId, CustomerName, VersionId, StartDate) 
	Values(1, 'Robert Smith', 2, GetDate() )
Select * from Customers
Go


-- Type 2 part c (aka: "I can't get enough of that sweet tracking stuff!")
-- You can just keep adding more information about the row changes by adding 
-- additional columns. Keep in mind though the when a SCD table
-- has a lot of changes over a short time (in otherwords it not all that slow)
-- you can create a performance issue for yourself.

Create -- Drop
Table Customers (
  CustomerId int
, CustomerName varchar(50)
, VersionId int
, StartDate DateTime
, EndDate DateTime
, AddedBy varchar(50)
Primary Key (CustomerId, VersionId ) 
)
Go

-- Add a new row
Insert into Customers (CustomerId, CustomerName, VersionId, StartDate, EndDate, AddedBy) 
	Values(1, 'Bob Smith', 0, GetDate(), Null, sUser_sName() )
Select * from Customers
Go


-- Of course, all this additional tracking demands additional programming...
-- Change an Existing row by 
-- 1) Updating the exiting row with the new EndDate
-- 2) Adding a new row with a new VersionId and StartDate  
Declare @Now DateTime
Declare @CustId int
Declare @VerId int

Select @CustId = 1, @VerId = 0, @Now = GetDate()

Update Customers
Set EndDate = @Now 
Where CustomerId = @CustId and @VerId = 0

Insert into Customers (CustomerId, CustomerName, VersionId, StartDate, EndDate, AddedBy ) 
	Values(@CustId, 'Robert Smith', @VerId + 1 , @Now, null, sUser_sName()  )
Select * from Customers
Go

-- Reset Demo --
Drop Table Customers
Use Master
Go