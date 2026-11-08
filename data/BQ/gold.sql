--1. Total Charge Amount per provider by department
CREATE TABLE IF NOT EXISTS `project-5d0d5583-f7fc-4cf4-9ce.gold_dataset.provider_charge_summary` (
    ProviderName STRING,
    DeptName STRING,
    Amount FLOAT64
);

# truncate table
TRUNCATE TABLE `project-5d0d5583-f7fc-4cf4-9ce.gold_dataset.provider_charge_summary`;

# insert data
INSERT INTO `project-5d0d5583-f7fc-4cf4-9ce.gold_dataset.provider_charge_summary`
SELECT 
    CONCAT(p.Firstname, ' ', p.LastName) AS ProviderName,
    d.Name AS DeptName,
    SUM(t.Amount) AS Amount
FROM `project-5d0d5583-f7fc-4cf4-9ce.silver_dataset.transactions` t
LEFT JOIN `project-5d0d5583-f7fc-4cf4-9ce.silver_dataset.providers` p 
    ON SPLIT(p.ProviderID, "-")[SAFE_OFFSET(1)] = t.ProviderID
LEFT JOIN `project-5d0d5583-f7fc-4cf4-9ce.silver_dataset.departments` d 
    ON SPLIT(d.DeptID, "-")[SAFE_OFFSET(0)] = p.DeptID
WHERE t.IsQuarantined = FALSE AND d.Name IS NOT NULL
GROUP BY ProviderName, DeptName;


--------------------------------------------------------------------------------------------------
--2. Patient History (Gold) : This table provides a complete history of a patient’s visits, diagnoses, and financial interactions.

# CREATE TABLE
CREATE TABLE IF NOT EXISTS `project-5d0d5583-f7fc-4cf4-9ce.gold_dataset.patient_history` (
    PatientKey STRING,
    FirstName STRING,
    LastName STRING,
    Gender STRING,
    DOB INT64,
    Address STRING,
    EncounterDate INT64,
    EncounterType STRING,
    TransactionKey STRING,
    VisitDate INT64,
    ServiceDate INT64,
    BilledAmount FLOAT64,
    PaidAmount FLOAT64,
    ClaimStatus STRING,
    ClaimAmount STRING,
    ClaimPaidAmount STRING,
    PayorType STRING
);


# TRUNCATE TABLE
TRUNCATE TABLE `project-5d0d5583-f7fc-4cf4-9ce.gold_dataset.patient_history`;

# INSERT DATA
INSERT INTO `project-5d0d5583-f7fc-4cf4-9ce.gold_dataset.patient_history`
SELECT 
    p.PatientKey,
    p.FirstName,
    p.LastName,
    p.Gender,
    p.DOB,
    p.Address,
    e.EncounterDate,
    e.EncounterType,
    t.TransactionKey,
    t.VisitDate,
    t.ServiceDate,
    t.Amount AS BilledAmount,
    t.PaidAmount,
    c.ClaimStatus,
    c.ClaimAmount,
    c.PaidAmount AS ClaimPaidAmount,
    c.PayorType
FROM `project-5d0d5583-f7fc-4cf4-9ce.silver_dataset.patients` p
LEFT JOIN `project-5d0d5583-f7fc-4cf4-9ce.silver_dataset.encounters` e 
    ON SPLIT(p.PatientKey, '-')[OFFSET(0)] || '-' || SPLIT(p.PatientKey, '-')[OFFSET(1)] = e.PatientID
LEFT JOIN `project-5d0d5583-f7fc-4cf4-9ce.silver_dataset.transactions` t 
    ON SPLIT(p.PatientKey, '-')[OFFSET(0)] || '-' || SPLIT(p.PatientKey, '-')[OFFSET(1)] = t.PatientID
LEFT JOIN `project-5d0d5583-f7fc-4cf4-9ce.silver_dataset.claims` c 
    ON t.SrcTransactionID = c.TransactionID
WHERE p.IsCurrent = TRUE;


--------------------------------------------------------------------------------------------------
-- 3. Provider Performance Summary (Gold) : This table summarizes provider activity, including the number of encounters, total billed amount, and claim success rate.

# CREATE TABLE
CREATE TABLE IF NOT EXISTS `project-5d0d5583-f7fc-4cf4-9ce.gold_dataset.provider_performance` (
    ProviderID STRING,
    FirstName STRING,
    LastName STRING,
    Specialization STRING,
    TotalEncounters INT64,
    TotalTransactions INT64,
    TotalBilledAmount FLOAT64,
    TotalPaidAmount FLOAT64,
    ApprovedClaims INT64,
    TotalClaims INT64,
    ClaimApprovalRate FLOAT64
);

# TRUNCATE TABLE
TRUNCATE TABLE `project-5d0d5583-f7fc-4cf4-9ce.gold_dataset.provider_performance`;

# INSERT DATA
INSERT INTO `project-5d0d5583-f7fc-4cf4-9ce.gold_dataset.provider_performance`
SELECT 
    pr.ProviderID,
    pr.FirstName,
    pr.LastName,
    pr.Specialization,
    COUNT(DISTINCT e.EncounterKey) AS TotalEncounters,
    COUNT(DISTINCT t.TransactionKey) AS TotalTransactions,
    SUM(t.Amount) AS TotalBilledAmount,
    SUM(t.PaidAmount) AS TotalPaidAmount,
    COUNT(DISTINCT CASE WHEN c.ClaimStatus = 'Approved' THEN c.ClaimKey END) AS ApprovedClaims,
    COUNT(DISTINCT c.ClaimKey) AS TotalClaims,
    ROUND((COUNT(DISTINCT CASE WHEN c.ClaimStatus = 'Approved' THEN c.ClaimKey END) / NULLIF(COUNT(DISTINCT c.ClaimKey), 0)) * 100, 2) AS ClaimApprovalRate
FROM `project-5d0d5583-f7fc-4cf4-9ce.silver_dataset.providers` pr
LEFT JOIN `project-5d0d5583-f7fc-4cf4-9ce.silver_dataset.encounters` e 
    ON SPLIT(pr.ProviderID, "-")[SAFE_OFFSET(1)] = e.ProviderID
LEFT JOIN `project-5d0d5583-f7fc-4cf4-9ce.silver_dataset.transactions` t 
    ON SPLIT(pr.ProviderID, "-")[SAFE_OFFSET(1)] = t.ProviderID
LEFT JOIN `project-5d0d5583-f7fc-4cf4-9ce.silver_dataset.claims` c 
    ON t.SrcTransactionID = c.TransactionID
GROUP BY pr.ProviderID, pr.FirstName, pr.LastName, pr.Specialization;

--------------------------------------------------------------------------------------------------
-- 4. Department Performance Analytics (Gold): Provides insights into department-level efficiency, revenue, and patient volume.

# CREATE TABLE
CREATE TABLE IF NOT EXISTS `project-5d0d5583-f7fc-4cf4-9ce.gold_dataset.department_performance` (
    DeptID STRING,
    DepartmentName STRING,
    TotalEncounters INT64,
    TotalTransactions INT64,
    TotalBilledAmount FLOAT64,
    TotalPaidAmount FLOAT64,
    AvgPaymentPerTransaction FLOAT64
);

# TRUNCATE TABLE
TRUNCATE TABLE `project-5d0d5583-f7fc-4cf4-9ce.gold_dataset.department_performance`;

# INSERT DATA
INSERT INTO `project-5d0d5583-f7fc-4cf4-9ce.gold_dataset.department_performance`
SELECT 
    d.DeptID,
    d.Name AS DepartmentName,
    COUNT(DISTINCT e.EncounterKey) AS TotalEncounters,
    COUNT(DISTINCT t.TransactionKey) AS TotalTransactions,
    SUM(t.Amount) AS TotalBilledAmount,
    SUM(t.PaidAmount) AS TotalPaidAmount,
    AVG(t.PaidAmount) AS AvgPaymentPerTransaction
FROM `project-5d0d5583-f7fc-4cf4-9ce.silver_dataset.departments` d
LEFT JOIN `project-5d0d5583-f7fc-4cf4-9ce.silver_dataset.encounters` e 
    ON SPLIT(d.DeptID, "-")[SAFE_OFFSET(0)] = e.DepartmentID
LEFT JOIN `project-5d0d5583-f7fc-4cf4-9ce.silver_dataset.transactions` t 
    ON SPLIT(d.DeptID, "-")[SAFE_OFFSET(0)] = t.DeptID
WHERE d.IsQuarantined = FALSE
GROUP BY d.DeptID, d.Name;

--------------------------------------------------------------------------------------------------

-- 5. Financial Metrics (Gold) : Aggregates financial KPIs, such as total revenue, claim success rate, and outstanding balances.

CREATE TABLE IF NOT EXISTS `project-5d0d5583-f7fc-4cf4-9ce.gold_dataset.financial_metrics` AS
SELECT 
    COUNT(DISTINCT t.TransactionKey) AS TotalTransactions,
    SUM(t.Amount) AS TotalBilledAmount,
    SUM(t.PaidAmount) AS TotalPaidAmount,
    SUM(t.Amount) - SUM(t.PaidAmount) AS OutstandingBalance,
    COUNT(DISTINCT CASE WHEN c.ClaimStatus = 'Approved' THEN c.ClaimKey END) AS ApprovedClaims,
    COUNT(DISTINCT c.ClaimKey) AS TotalClaims,
    ROUND((COUNT(DISTINCT CASE WHEN c.ClaimStatus = 'Approved' THEN c.ClaimKey END) / NULLIF(COUNT(DISTINCT c.ClaimKey), 0)) * 100, 2) AS ClaimApprovalRate
FROM `project-5d0d5583-f7fc-4cf4-9ce.silver_dataset.transactions` t
LEFT JOIN `project-5d0d5583-f7fc-4cf4-9ce.silver_dataset.claims` c 
    ON t.SrcTransactionID = c.TransactionID
WHERE t.IsCurrent = TRUE;

-- --------------------------------------------------------------------------------------------------

--6. Payor Performance & Claims Summary (Gold): This table tracks the performance of insurance payors, focusing on claim approval rates, payout amounts, and processing efficiency.

CREATE TABLE IF NOT EXISTS `project-5d0d5583-f7fc-4cf4-9ce.gold_dataset.payor_performance` AS
SELECT 
    c.PayorID,
    c.PayorType,
    COUNT(DISTINCT c.ClaimKey) AS TotalClaims,
    COUNT(DISTINCT CASE WHEN c.ClaimStatus = 'Approved' THEN c.ClaimKey END) AS ApprovedClaims,
    COUNT(DISTINCT CASE WHEN c.ClaimStatus = 'Denied' THEN c.ClaimKey END) AS DeniedClaims,
    COUNT(DISTINCT CASE WHEN c.ClaimStatus = 'Pending' THEN c.ClaimKey END) AS PendingClaims,
    ROUND((COUNT(DISTINCT CASE WHEN c.ClaimStatus = 'Approved' THEN c.ClaimKey END) / NULLIF(COUNT(DISTINCT c.ClaimKey), 0)) * 100, 2) AS ClaimApprovalRate,
    SUM(CAST(c.ClaimAmount AS FLOAT64)) AS TotalClaimAmount,
    SUM(CAST(c.PaidAmount AS FLOAT64)) AS TotalPaidAmount,
    SUM(CAST(c.ClaimAmount AS FLOAT64)) - SUM(CAST(c.PaidAmount AS FLOAT64)) AS OutstandingAmount
FROM `project-5d0d5583-f7fc-4cf4-9ce.silver_dataset.claims` c
WHERE c.IsCurrent = TRUE
GROUP BY c.PayorID, c.PayorType;
