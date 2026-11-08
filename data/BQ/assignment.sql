-- 4. Financial Metrics (Gold) : Aggregates financial KPIs, such as total revenue, claim success rate, and outstanding balances.

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

--5. Payor Performance & Claims Summary (Gold): This table tracks the performance of insurance payors, focusing on claim approval rates, payout amounts, and processing efficiency.

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
