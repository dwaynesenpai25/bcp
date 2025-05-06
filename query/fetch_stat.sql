WITH RankedDispositions AS (
    SELECT
        debtor.id AS 'DEBTOR ID',
        debtor.account AS 'ACCOUNT NUMBER',
        debtor.name AS 'NAME',
        followup.status_code AS 'STATUS CODE',
        followup.remark AS 'REMARKS',
        followup.remark_by AS 'REMARKS BY',
        followup.contact_number AS 'PHONE',
        followup.datetime AS 'BARCODE DATE',
        debtor_followup.claim_paid_amount AS 'CLAIM PAID AMOUNT',
        debtor_followup.claim_paid_date AS 'CLAIM PAID DATE',
        debtor_followup.ptp_amount AS 'PTP AMOUNT',
        debtor_followup.ptp_date AS 'PTP DATE',
        ROW_NUMBER() OVER (PARTITION BY debtor.id ORDER BY followup.datetime DESC) AS rn
    FROM debtor
        LEFT JOIN debtor_followup ON debtor_followup.debtor_id = debtor.id
        LEFT JOIN followup ON followup.id = debtor_followup.followup_id
        LEFT JOIN `client` ON client.id = debtor.client_id
    WHERE client.id IN ({selected_client_id})
        AND debtor.id IN ({id_list})
        AND followup.datetime IS NOT NULL
)
SELECT
    `DEBTOR ID`,
    `ACCOUNT NUMBER`,  
    NAME,
    `STATUS CODE`,
    REMARKS,
    `REMARKS BY`,
    PHONE,
    `BARCODE DATE`,
    `CLAIM PAID AMOUNT`,
    `CLAIM PAID DATE`,
    `PTP AMOUNT`,
    `PTP DATE`
FROM RankedDispositions
WHERE rn <= 5;