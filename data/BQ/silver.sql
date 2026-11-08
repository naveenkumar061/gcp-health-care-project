-- IN THIS WE WE WILL IMPLEMENTING BOTH SCD2 AND CDM LOGIC FOR THE SILVER TABLES

-- departments ==> full load + cdm
-- 1. Create Table Departments by Merge Data From Hospital A and B
CREATE TABLE IF NOT EXISTS `project-5d0d5583-f7fc-4cf4-9ce.silver_dataset.departments`(
  DeptId STRING,
  SrcDeptId STRING,
  Name STRING,
  DataSource STRING,
  IsQuarantined BOOLEAN);

-- 2.Truncate Silver Table Before Inserting
TRUNCATE TABLE `project-5d0d5583-f7fc-4cf4-9ce.silver_dataset.departments`;

-- 3.Full Load By Inserting Merged Data
INSERT INTO `project-5d0d5583-f7fc-4cf4-9ce.silver_dataset.departments`
SELECT DISTINCT
  CONCAT(DeptID, '-', DataSource) AS DeptID,
  DeptID AS SrcDeptID,
  Name,
  DataSource,
  CASE
    WHEN DeptID IS NULL OR Name IS NULL THEN TRUE
    ELSE FALSE
    END AS IsQuarantined
FROM
  (
    SELECT DISTINCT *, 'hosa' AS DataSource
    FROM `project-5d0d5583-f7fc-4cf4-9ce.bronze_dataset.departments_ha`
    UNION ALL
    SELECT DISTINCT *, 'hosb' AS DataSource
    FROM `project-5d0d5583-f7fc-4cf4-9ce.bronze_dataset.departments_hb`
  );

-------------------------------------------------------------------------------------------------------
-- 1. Create table providers by Merge Data from Hospital A & B
CREATE TABLE IF NOT EXISTS `project-5d0d5583-f7fc-4cf4-9ce.silver_dataset.providers`(
  ProviderID STRING,
  FirstName STRING,
  LastName STRING,
  Specialization STRING,
  DeptID STRING,
  NPI INT64,
  DataSource STRING,
  IsQuarantined BOOLEAN);

-- 2. Truncate Silver Table Before Inserting
TRUNCATE TABLE `project-5d0d5583-f7fc-4cf4-9ce.silver_dataset.providers`;

-- 3. full load by Inserting merged Data
INSERT INTO `project-5d0d5583-f7fc-4cf4-9ce.silver_dataset.providers`
SELECT DISTINCT
  ProviderID,
  FirstName,
  LastName,
  Specialization,
  DeptID,
  CAST(NPI AS INT64) AS NPI,
  DataSource,
  CASE
    WHEN ProviderID IS NULL OR DeptID IS NULL THEN TRUE
    ELSE FALSE
    END AS IsQuarantined
FROM
  (
    SELECT DISTINCT *, 'hosa' AS DataSource
    FROM `project-5d0d5583-f7fc-4cf4-9ce.bronze_dataset.providers_ha`
    UNION ALL
    SELECT DISTINCT *, 'hosb' AS DataSource
    FROM `project-5d0d5583-f7fc-4cf4-9ce.bronze_dataset.providers_hb`
  );

-------------------------------------------------------------------------------------------------------

-- 1. Create patients Table in BigQuery
CREATE TABLE IF NOT EXISTS `project-5d0d5583-f7fc-4cf4-9ce.silver_dataset.patients`(
  PatientKey STRING,
  SrcPatientID STRING,
  FirstName STRING,
  LastName STRING,
  MiddleName STRING,
  SSN STRING,
  PhoneNumber STRING,
  Gender STRING,
  DOB INT64,
  Address STRING,
  SrcModifiedDate INT64,
  DataSource STRING,
  IsQuarantined BOOL,
  InsertedDate TIMESTAMP,
  ModifiedDate TIMESTAMP,
  IsCurrent BOOL);

-- 2.Create a quality_checks temp table for patients
CREATE OR REPLACE TABLE `project-5d0d5583-f7fc-4cf4-9ce.silver_dataset.quality_checks_patients`
AS
SELECT DISTINCT
  CONCAT(SrcPatientID, '-', DataSource) AS PatientKey,
  SrcPatientID,
  FirstName,
  LastName,
  MiddleName,
  SSN,
  PhoneNumber,
  Gender,
  DOB,
  Address,
  ModifiedDate AS SrcModifiedDate,
  DataSource,
  CASE
    WHEN
      SrcPatientID IS NULL
      OR DOB IS NULL
      OR FirstName IS NULL
      OR LOWER(FirstName) = 'null'
      THEN TRUE
    ELSE FALSE
    END AS IsQuarantined
FROM
  (
    SELECT DISTINCT
      PatientID AS SrcPatientID,
      FirstName,
      LastName,
      MiddleName,
      SSN,
      PhoneNumber,
      Gender,
      DOB,
      Address,
      ModifiedDate,
      'hosa' AS DataSource
    FROM `project-5d0d5583-f7fc-4cf4-9ce.bronze_dataset.patients_ha`
    UNION ALL
    SELECT DISTINCT
      ID AS SrcPatientID,
      F_Name AS FirstName,
      L_Name AS LastName,
      M_Name AS MiddleName,
      SSN,
      PhoneNumber,
      Gender,
      DOB,
      Address,
      ModifiedDate,
      'hosb' AS DataSource
    FROM `project-5d0d5583-f7fc-4cf4-9ce.bronze_dataset.patients_hb`
  );

-- 3. Apply SCD Type 2 Logic with MERGE
MERGE INTO `project-5d0d5583-f7fc-4cf4-9ce.silver_dataset.patients` AS Target
USING
  `project-5d0d5583-f7fc-4cf4-9ce.silver_dataset.quality_checks_patients`
    AS Source
ON
  Target.PatientKey = Source.PatientKey
  AND Target.IsCurrent
    = TRUE

      -- Step 1: Mark existing records as historical if any column has changed
      WHEN
        MATCHED
        AND (
          Target.SrcPatientID <> Source.SrcPatientID
          OR Target.FirstName <> Source.FirstName
          OR Target.LastName <> Source.LastName
          OR Target.MiddleName <> Source.MiddleName
          OR Target.SSN <> Source.SSN
          OR Target.PhoneNumber <> Source.PhoneNumber
          OR Target.Gender <> Source.Gender
          OR Target.DOB <> Source.DOB
          OR Target.Address <> Source.Address
          OR Target.SrcModifiedDate <> Source.SrcModifiedDate
          OR Target.DataSource <> Source.DataSource
          OR Target.IsQuarantined <> Source.IsQuarantined)
        THEN UPDATE
SET
  Target.IsCurrent = FALSE,
  Target.ModifiedDate = CURRENT_TIMESTAMP()

    -- Step 2: Insert new and updated records as the latest active records
    WHEN NOT MATCHED
      THEN
        INSERT(
          PatientKey,
          SrcPatientID,
          FirstName,
          LastName,
          MiddleName,
          SSN,
          PhoneNumber,
          Gender,
          DOB,
          Address,
          SrcModifiedDate,
          DataSource,
          IsQuarantined,
          InsertedDate,
          ModifiedDate,
          IsCurrent)
          VALUES(
            Source.PatientKey,
            Source.SrcPatientID,
            Source.FirstName,
            Source.LastName,
            Source.MiddleName,
            Source.SSN,
            Source.PhoneNumber,
            Source.Gender,
            Source.DOB,
            Source.Address,
            Source.SrcModifiedDate,
            Source.DataSource,
            Source.IsQuarantined,
            CURRENT_TIMESTAMP(),
            CURRENT_TIMESTAMP(),
            TRUE);

-- DROP quality_check table
DROP TABLE IF EXISTS `project-5d0d5583-f7fc-4cf4-9ce.silver_dataset.quality_checks_patients`;

-------------------------------------------------------------------------------------------------------

-- 1. Create transactions Table in BigQuery
CREATE TABLE IF NOT EXISTS `project-5d0d5583-f7fc-4cf4-9ce.silver_dataset.transactions`(
  TransactionKey STRING,
  SrcTransactionID STRING,
  EncounterID STRING,
  PatientID STRING,
  ProviderID STRING,
  DeptID STRING,
  VisitDate INT64,
  ServiceDate INT64,
  PaidDate INT64,
  VisitType STRING,
  Amount FLOAT64,
  AmountType STRING,
  PaidAmount FLOAT64,
  ClaimID STRING,
  PayorID STRING,
  ProcedureCode INT64,
  ICDCode STRING,
  LineOfBusiness STRING,
  MedicaidID STRING,
  MedicareID STRING,
  SrcInsertDate INT64,
  SrcModifiedDate INT64,
  DataSource STRING,
  IsQuarantined BOOL,
  InsertedDate TIMESTAMP,
  ModifiedDate TIMESTAMP,
  IsCurrent BOOL);

-- 2. Create a quality_checks temp table
CREATE OR REPLACE TABLE `project-5d0d5583-f7fc-4cf4-9ce.silver_dataset.quality_checks_transactions`
AS
SELECT DISTINCT
  CONCAT(TransactionID, '-', DataSource) AS TransactionKey,
  TransactionID AS SrcTransactionID,
  EncounterID,
  PatientID,
  ProviderID,
  DeptID,
  VisitDate,
  ServiceDate,
  PaidDate,
  VisitType,
  Amount,
  AmountType,
  PaidAmount,
  ClaimID,
  PayorID,
  ProcedureCode,
  ICDCode,
  LineOfBusiness,
  MedicaidID,
  MedicareID,
  InsertDate AS SrcInsertDate,
  ModifiedDate AS SrcModifiedDate,
  DataSource,
  CASE
    WHEN
      EncounterID IS NULL
      OR PatientID IS NULL
      OR TransactionID IS NULL
      OR VisitDate IS NULL
      THEN TRUE
    ELSE FALSE
    END AS IsQuarantined
FROM
  (
    SELECT DISTINCT *, 'hosa' AS DataSource
    FROM `project-5d0d5583-f7fc-4cf4-9ce.bronze_dataset.transactions_ha`
    UNION ALL
    SELECT DISTINCT *, 'hosb' AS DataSource
    FROM `project-5d0d5583-f7fc-4cf4-9ce.bronze_dataset.transactions_hb`
  );

-- 3. Apply SCD Type 2 Logic with MERGE
MERGE
  INTO `project-5d0d5583-f7fc-4cf4-9ce.silver_dataset.transactions` AS Target
USING `project-5d0d5583-f7fc-4cf4-9ce.silver_dataset.quality_checks_transactions` AS Source
ON
  Target.TransactionKey = Source.TransactionKey
  AND Target.IsCurrent
    = TRUE

      -- Step 1: Mark existing records as historical if any column has changed
      WHEN
        MATCHED
        AND (
          Target.SrcTransactionID <> Source.SrcTransactionID
          OR Target.EncounterID <> Source.EncounterID
          OR Target.PatientID <> Source.PatientID
          OR Target.ProviderID <> Source.ProviderID
          OR Target.DeptID <> Source.DeptID
          OR Target.VisitDate <> Source.VisitDate
          OR Target.ServiceDate <> Source.ServiceDate
          OR Target.PaidDate <> Source.PaidDate
          OR Target.VisitType <> Source.VisitType
          OR Target.Amount <> Source.Amount
          OR Target.AmountType <> Source.AmountType
          OR Target.PaidAmount <> Source.PaidAmount
          OR Target.ClaimID <> Source.ClaimID
          OR Target.PayorID <> Source.PayorID
          OR Target.ProcedureCode <> Source.ProcedureCode
          OR Target.ICDCode <> Source.ICDCode
          OR Target.LineOfBusiness <> Source.LineOfBusiness
          OR Target.MedicaidID <> Source.MedicaidID
          OR Target.MedicareID <> Source.MedicareID
          OR Target.SrcInsertDate <> Source.SrcInsertDate
          OR Target.SrcModifiedDate <> Source.SrcModifiedDate
          OR Target.DataSource <> Source.DataSource
          OR Target.IsQuarantined <> Source.IsQuarantined)
        THEN UPDATE
SET
  Target.IsCurrent = FALSE,
  Target.ModifiedDate = CURRENT_TIMESTAMP()

    -- Step 2: Insert new and updated records as the latest active records
    WHEN NOT MATCHED
      THEN
        INSERT(
          TransactionKey,
          SrcTransactionID,
          EncounterID,
          PatientID,
          ProviderID,
          DeptID,
          VisitDate,
          ServiceDate,
          PaidDate,
          VisitType,
          Amount,
          AmountType,
          PaidAmount,
          ClaimID,
          PayorID,
          ProcedureCode,
          ICDCode,
          LineOfBusiness,
          MedicaidID,
          MedicareID,
          SrcInsertDate,
          SrcModifiedDate,
          DataSource,
          IsQuarantined,
          InsertedDate,
          ModifiedDate,
          IsCurrent)
          VALUES(
            Source.TransactionKey,
            Source.SrcTransactionID,
            Source.EncounterID,
            Source.PatientID,
            Source.ProviderID,
            Source.DeptID,
            Source.VisitDate,
            Source.ServiceDate,
            Source.PaidDate,
            Source.VisitType,
            Source.Amount,
            Source.AmountType,
            Source.PaidAmount,
            Source.ClaimID,
            Source.PayorID,
            Source.ProcedureCode,
            Source.ICDCode,
            Source.LineOfBusiness,
            Source.MedicaidID,
            Source.MedicareID,
            Source.SrcInsertDate,
            Source.SrcModifiedDate,
            Source.DataSource,
            Source.IsQuarantined,
            CURRENT_TIMESTAMP(),
            CURRENT_TIMESTAMP(),
            TRUE);

-- 4. DROP quality_check table
DROP TABLE IF EXISTS `project-5d0d5583-f7fc-4cf4-9ce.silver_dataset.quality_checks_transactions`;

-------------------------------------------------------------------------------------------------------

-- 1. Create the encounters Table in BigQuery
CREATE TABLE IF NOT EXISTS `project-5d0d5583-f7fc-4cf4-9ce.silver_dataset.encounters`(
  EncounterKey STRING,
  SrcEncounterID STRING,
  PatientID STRING,
  ProviderID STRING,
  DepartmentID STRING,
  EncounterDate INT64,
  EncounterType STRING,
  ProcedureCode INT64,
  SrcModifiedDate INT64,
  DataSource STRING,
  IsQuarantined BOOL,
  InsertedDate TIMESTAMP,
  ModifiedDate TIMESTAMP,
  IsCurrent BOOL);

-- 2. Create a quality_checks temp table for encounters
CREATE OR REPLACE TABLE `project-5d0d5583-f7fc-4cf4-9ce.silver_dataset.quality_checks_encounters`
AS
SELECT DISTINCT
  CONCAT(SrcEncounterID, '-', DataSource) AS EncounterKey,
  SrcEncounterID,
  PatientID,
  ProviderID,
  DepartmentID,
  EncounterDate,
  EncounterType,
  ProcedureCode,
  ModifiedDate AS SrcModifiedDate,
  DataSource,
  CASE
    WHEN
      SrcEncounterID IS NULL
      OR PatientID IS NULL
      OR EncounterDate IS NULL
      OR LOWER(EncounterType) = 'null'
      THEN TRUE
    ELSE FALSE
    END AS IsQuarantined
FROM
  (
    SELECT DISTINCT
      EncounterID AS SrcEncounterID,
      PatientID,
      ProviderID,
      DepartmentID,
      EncounterDate,
      EncounterType,
      ProcedureCode,
      ModifiedDate,
      'hosa' AS DataSource
    FROM `project-5d0d5583-f7fc-4cf4-9ce.bronze_dataset.encounters_ha`
    UNION ALL
    SELECT DISTINCT
      EncounterID AS SrcEncounterID,
      PatientID,
      ProviderID,
      DepartmentID,
      EncounterDate,
      EncounterType,
      ProcedureCode,
      ModifiedDate,
      'hosb' AS DataSource
    FROM `project-5d0d5583-f7fc-4cf4-9ce.bronze_dataset.encounters_hb`
  );

-- 3. Apply SCD Type 2 Logic with MERGE
MERGE INTO `project-5d0d5583-f7fc-4cf4-9ce.silver_dataset.encounters` AS Target
USING
  `project-5d0d5583-f7fc-4cf4-9ce.silver_dataset.quality_checks_encounters`
    AS Source
ON
  Target.EncounterKey = Source.EncounterKey
  AND Target.IsCurrent
    = TRUE

      -- Step 1: Mark existing records as historical if any column has changed
      WHEN
        MATCHED
        AND (
          Target.SrcEncounterID <> Source.SrcEncounterID
          OR Target.PatientID <> Source.PatientID
          OR Target.ProviderID <> Source.ProviderID
          OR Target.DepartmentID <> Source.DepartmentID
          OR Target.EncounterDate <> Source.EncounterDate
          OR Target.EncounterType <> Source.EncounterType
          OR Target.ProcedureCode <> Source.ProcedureCode
          OR Target.SrcModifiedDate <> Source.SrcModifiedDate
          OR Target.DataSource <> Source.DataSource
          OR Target.IsQuarantined <> Source.IsQuarantined)
        THEN UPDATE
SET
  Target.IsCurrent = FALSE,
  Target.ModifiedDate = CURRENT_TIMESTAMP()

    -- Step 2: Insert new and updated records as the latest active records
    WHEN NOT MATCHED
      THEN
        INSERT(
          EncounterKey,
          SrcEncounterID,
          PatientID,
          ProviderID,
          DepartmentID,
          EncounterDate,
          EncounterType,
          ProcedureCode,
          SrcModifiedDate,
          DataSource,
          IsQuarantined,
          InsertedDate,
          ModifiedDate,
          IsCurrent)
          VALUES(
            Source.EncounterKey,
            Source.SrcEncounterID,
            Source.PatientID,
            Source.ProviderID,
            Source.DepartmentID,
            Source.EncounterDate,
            Source.EncounterType,
            Source.ProcedureCode,
            Source.SrcModifiedDate,
            Source.DataSource,
            Source.IsQuarantined,
            CURRENT_TIMESTAMP(),
            CURRENT_TIMESTAMP(),
            TRUE);

-- 4. DROP quality_check table
DROP TABLE IF EXISTS `project-5d0d5583-f7fc-4cf4-9ce.silver_dataset.quality_checks_encounters`;

-------------------------------------------------------------------------------------------------------

-- 1. Create the Claims Table in BigQuery
CREATE TABLE IF NOT EXISTS `project-5d0d5583-f7fc-4cf4-9ce.silver_dataset.claims`(
  ClaimKey STRING,
  SrcClaimID STRING,
  TransactionID STRING,
  PatientID STRING,
  EncounterID STRING,
  ProviderID STRING,
  DeptID STRING,
  ServiceDate STRING,
  ClaimDate STRING,
  PayorID STRING,
  ClaimAmount STRING,
  PaidAmount STRING,
  ClaimStatus STRING,
  PayorType STRING,
  Deductible STRING,
  Coinsurance STRING,
  Copay STRING,
  SrcInsertDate STRING,
  SrcModifiedDate STRING,
  DataSource STRING,
  IsQuarantined BOOLEAN,
  InsertedDate TIMESTAMP,
  ModifiedDate TIMESTAMP,
  IsCurrent BOOLEAN);

-- 2. Create a quality_checks temp table for claims
CREATE OR REPLACE TABLE `project-5d0d5583-f7fc-4cf4-9ce.silver_dataset.quality_checks_claims`
AS
SELECT
  CONCAT(SrcClaimID, '-', DataSource) AS ClaimKey,
  SrcClaimID,
  TransactionID,
  PatientID,
  EncounterID,
  ProviderID,
  DeptID,
  ServiceDate,
  ClaimDate,
  PayorID,
  ClaimAmount,
  PaidAmount,
  ClaimStatus,
  PayorType,
  Deductible,
  Coinsurance,
  Copay,
  InsertDate AS SrcInsertDate,
  ModifiedDate AS SrcModifiedDate,
  DataSource,
  CASE
    WHEN
      SrcClaimID IS NULL
      OR PatientID IS NULL
      OR TransactionID IS NULL
      OR LOWER(ClaimStatus) = 'null'
      THEN TRUE
    ELSE FALSE
    END AS IsQuarantined
FROM
  (
    SELECT
      ClaimID AS SrcClaimID,
      TransactionID,
      PatientID,
      EncounterID,
      ProviderID,
      DeptID,
      ServiceDate,
      ClaimDate,
      PayorID,
      ClaimAmount,
      PaidAmount,
      ClaimStatus,
      PayorType,
      Deductible,
      Coinsurance,
      Copay,
      InsertDate,
      ModifiedDate,
      'hosa' AS DataSource
    FROM `project-5d0d5583-f7fc-4cf4-9ce.bronze_dataset.claims`
  );

-- 3. Apply SCD Type 2 Logic with MERGE
MERGE INTO `project-5d0d5583-f7fc-4cf4-9ce.silver_dataset.claims` AS Target
USING
  `project-5d0d5583-f7fc-4cf4-9ce.silver_dataset.quality_checks_claims`
    AS Source
ON
  Target.ClaimKey = Source.ClaimKey
  AND Target.IsCurrent
    = TRUE

      -- Step 1: Mark existing records as historical if any column has changed
      WHEN
        MATCHED
        AND (
          Target.SrcClaimID <> Source.SrcClaimID
          OR Target.TransactionID <> Source.TransactionID
          OR Target.PatientID <> Source.PatientID
          OR Target.EncounterID <> Source.EncounterID
          OR Target.ProviderID <> Source.ProviderID
          OR Target.DeptID <> Source.DeptID
          OR Target.ServiceDate <> Source.ServiceDate
          OR Target.ClaimDate <> Source.ClaimDate
          OR Target.PayorID <> Source.PayorID
          OR Target.ClaimAmount <> Source.ClaimAmount
          OR Target.PaidAmount <> Source.PaidAmount
          OR Target.ClaimStatus <> Source.ClaimStatus
          OR Target.PayorType <> Source.PayorType
          OR Target.Deductible <> Source.Deductible
          OR Target.Coinsurance <> Source.Coinsurance
          OR Target.Copay <> Source.Copay
          OR Target.SrcModifiedDate <> Source.SrcModifiedDate
          OR Target.DataSource <> Source.DataSource
          OR Target.IsQuarantined <> Source.IsQuarantined)
        THEN UPDATE
SET
  Target.IsCurrent = FALSE,
  Target.ModifiedDate = CURRENT_TIMESTAMP()

    -- Step 2: Insert new and updated records as the latest active records
    WHEN NOT MATCHED
      THEN
        INSERT(
          ClaimKey,
          SrcClaimID,
          TransactionID,
          PatientID,
          EncounterID,
          ProviderID,
          DeptID,
          ServiceDate,
          ClaimDate,
          PayorID,
          ClaimAmount,
          PaidAmount,
          ClaimStatus,
          PayorType,
          Deductible,
          Coinsurance,
          Copay,
          SrcInsertDate,
          SrcModifiedDate,
          DataSource,
          IsQuarantined,
          InsertedDate,
          ModifiedDate,
          IsCurrent)
          VALUES(
            Source.ClaimKey,
            Source.SrcClaimID,
            Source.TransactionID,
            Source.PatientID,
            Source.EncounterID,
            Source.ProviderID,
            Source.DeptID,
            Source.ServiceDate,
            Source.ClaimDate,
            Source.PayorID,
            Source.ClaimAmount,
            Source.PaidAmount,
            Source.ClaimStatus,
            Source.PayorType,
            Source.Deductible,
            Source.Coinsurance,
            Source.Copay,
            Source.SrcInsertDate,
            Source.SrcModifiedDate,
            Source.DataSource,
            Source.IsQuarantined,
            CURRENT_TIMESTAMP(),
            CURRENT_TIMESTAMP(),
            TRUE);

-- 4. DROP quality_check table
DROP TABLE IF EXISTS `project-5d0d5583-f7fc-4cf4-9ce.silver_dataset.quality_checks_claims`;

-------------------------------------------------------------------------------------------------------

-- 1. Create the CP Codes Silver Table in BigQuery
CREATE TABLE IF NOT EXISTS `project-5d0d5583-f7fc-4cf4-9ce.silver_dataset.cpt_codes`(
  CP_Code_Key STRING,
  procedure_code_category STRING,
  cpt_codes STRING,
  procedure_code_descriptions STRING,
  code_status STRING,
  DataSource STRING,
  IsQuarantined BOOLEAN,
  InsertedDate TIMESTAMP,
  ModifiedDate TIMESTAMP,
  IsCurrent BOOLEAN);

-- 2. Create a quality_checks temp table for CP Codes
CREATE OR REPLACE TABLE `project-5d0d5583-f7fc-4cf4-9ce.silver_dataset.quality_checks_cpt_codes`
AS
SELECT
  CONCAT(cpt_codes, '-', DataSource) AS CP_Code_Key,
  procedure_code_category,
  cpt_codes,
  procedure_code_descriptions,
  code_status,
  DataSource,
  -- Define a quarantine condition (null values in key fields)
  CASE
    WHEN cpt_codes IS NULL OR LOWER(code_status) = 'null' THEN TRUE
    ELSE FALSE
    END AS IsQuarantined
FROM
  (
    SELECT
      procedure_code_category,
      cpt_codes,
      procedure_code_descriptions,
      code_status,
      'hosa' AS DataSource
    FROM `project-5d0d5583-f7fc-4cf4-9ce.bronze_dataset.cpt_codes`
  );

-- 3. Apply SCD Type 2 Logic with MERGE
MERGE INTO `project-5d0d5583-f7fc-4cf4-9ce.silver_dataset.cpt_codes` AS Target
USING
  `project-5d0d5583-f7fc-4cf4-9ce.silver_dataset.quality_checks_cpt_codes`
    AS Source
ON
  Target.CP_Code_Key = Source.CP_Code_Key
  AND Target.IsCurrent
    = TRUE

      -- Step 1: Mark existing records as historical if any column has changed
      WHEN
        MATCHED
        AND (
          Target.procedure_code_category <> Source.procedure_code_category
          OR Target.cpt_codes <> Source.cpt_codes
          OR Target.procedure_code_descriptions
            <> Source.procedure_code_descriptions
          OR Target.code_status <> Source.code_status
          OR Target.DataSource <> Source.DataSource
          OR Target.IsQuarantined <> Source.IsQuarantined)
        THEN UPDATE
SET
  Target.IsCurrent = FALSE,
  Target.ModifiedDate = CURRENT_TIMESTAMP()

    -- Step 2: Insert new and updated records as the latest active records
    WHEN NOT MATCHED
      THEN
        INSERT(
          CP_Code_Key,
          procedure_code_category,
          cpt_codes,
          procedure_code_descriptions,
          code_status,
          DataSource,
          IsQuarantined,
          InsertedDate,
          ModifiedDate,
          IsCurrent)
          VALUES(
            Source.CP_Code_Key,
            Source.procedure_code_category,
            Source.cpt_codes,
            Source.procedure_code_descriptions,
            Source.code_status,
            Source.DataSource,
            Source.IsQuarantined,
            CURRENT_TIMESTAMP(),
            CURRENT_TIMESTAMP(),
            TRUE);

-- 4. DROP quality_check table
DROP TABLE IF EXISTS `project-5d0d5583-f7fc-4cf4-9ce.silver_dataset.quality_checks_cpt_codes`;
