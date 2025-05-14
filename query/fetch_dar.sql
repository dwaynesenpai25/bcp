WITH RankedDispositions AS (
    SELECT
        debtor.id AS 'ch_code',
        followup.datetime AS "RESULT DATE",
        debtor.collector_user_name AS "AGENT",
        followup.status_code AS "STATUS CODE",
        SUBSTRING_INDEX(followup.status_code, ' - ', 1) AS 'DISPOSITION',
        SUBSTRING_INDEX(followup.status_code, ' - ', -1) AS 'SUB DISPOSITION',
        debtor.balance AS "AMOUNT",
        debtor_followup.ptp_amount AS 'PTP AMOUNT',
        DATE_FORMAT(debtor_followup.ptp_date, '%d/%m/%Y') AS 'PTP DATE',
        debtor_followup.claim_paid_amount AS 'CLAIM PAID AMOUNT',
        DATE_FORMAT(debtor_followup.claim_paid_date, '%d/%m/%Y') AS 'CLAIM PAID DATE',
        followup.remark AS "NOTES",
        followup.contact_number AS "NUMBER CONTACTED",
        followup.remark_by AS "BARCODED BY",
        CASE  
            WHEN followup.remark_type_id = 1 THEN 'Follow Up' 
            WHEN followup.remark_type_id = 2 THEN 'Internal Remark'  
            WHEN followup.remark_type_id = 3 THEN 'Payment'
            WHEN followup.remark_type_id = 4 THEN 'SMS'
            WHEN followup.remark_type_id = 5 THEN 'Field Visit'
            WHEN followup.remark_type_id = 6 THEN 'Legal'
            WHEN followup.remark_type_id = 7 THEN 'Letter Attachment & Email'
            WHEN followup.remark_type_id = 9 THEN 'Permanent Message'
        END AS 'CONTACT SOURCE',
        ROW_NUMBER() OVER (PARTITION BY debtor.id ORDER BY followup.datetime DESC) AS rn
    FROM debtor
        LEFT JOIN debtor_followup ON debtor_followup.debtor_id = debtor.id
        LEFT JOIN followup ON followup.id = debtor_followup.followup_id
        LEFT JOIN `client` ON client.id = debtor.client_id
    WHERE client.id IN ({selected_client_id})
        AND debtor.id IN ({id_list})
        -- AND followup.datetime IS NOT NULL
)
SELECT
    ch_code,
    `RESULT DATE`,  
    AGENT,
    `STATUS CODE`,
    DISPOSITION,
    `SUB DISPOSITION`,
    AMOUNT,
    `PTP AMOUNT`,
    `PTP DATE`,
    `CLAIM PAID AMOUNT`,
    `CLAIM PAID DATE`,
    NOTES,
    `NUMBER CONTACTED`,
    `BARCODED BY`,
    `CONTACT SOURCE`
FROM RankedDispositions
WHERE rn <= 10;
