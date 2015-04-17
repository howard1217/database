DROP VIEW IF EXISTS q1a, q1b, q1c, q1d, q2, q3, q4, q5, q6, q7;

-- Question 1a
CREATE VIEW q1a(id, amount)
AS
  SELECT cmte_id, transaction_amt
  FROM committee_contributions
  WHERE transaction_amt > 5000
;

-- Question 1b
CREATE VIEW q1b(id, name, amount)
AS
  SELECT cmte_id, name, transaction_amt
  FROM committee_contributions
  WHERE transaction_amt > 5000
;

-- Question 1c
CREATE VIEW q1c(id, name, avg_amount)
AS
  SELECT cmte_id, name, AVG(transaction_amt)
  FROM committee_contributions
  WHERE transaction_amt > 5000
  GROUP BY cmte_id, name
;

-- Question 1d
CREATE VIEW q1d(id, name, avg_amount)
AS
  SELECT cmte_id, name, AVG(transaction_amt)
  FROM committee_contributions
  WHERE transaction_amt > 5000
  GROUP BY cmte_id, name
  HAVING AVG(transaction_amt) > 10000
;

-- Question 2
CREATE VIEW q2(from_name, to_name)
AS
  SELECT c1.name, c2.name
  -- from = other_id, to = cmte_id
  FROM intercommittee_transactions cc 
    INNER JOIN committees c1 ON cc.other_id = c1.id
    INNER JOIN committees c2 ON cc.cmte_id = c2.id
  WHERE c1.pty_affiliation = 'DEM' AND c2.pty_affiliation = 'DEM'
  GROUP BY c1.id, c2.id
  ORDER BY COUNT (*) desc
  LIMIT 10
;

-- Question 3
CREATE VIEW q3(name)
AS
  SELECT c.name
  FROM committees c
  WHERE c.id NOT IN
    (SELECT cc.cmte_id
     FROM committee_contributions cc
       INNER JOIN candidates cand ON cand.id = cc.cand_id
     WHERE cand.name = 'OBAMA, BARACK')
;

-- Question 4.
CREATE VIEW q4 (name)
AS
  WITH NumLimit(num) AS
    (SELECT COUNT(DISTINCT id)*0.01 FROM committees)
  SELECT cand.name
  FROM candidates cand
  WHERE cand.id IN
    (SELECT cc.cand_id
      FROM committee_contributions cc
        INNER JOIN candidates cand ON cand.id = cc.cand_id
      GROUP BY cc.cand_id
      HAVING COUNT(DISTINCT cc.cmte_id) > (SELECT * FROM NumLimit))
;

-- Question 5
CREATE VIEW q5 (name, total_pac_donations) AS
  WITH ORGContribution(cmte_id, transaction_amt) AS
  (SELECT cmte_id, transaction_amt
   FROM individual_contributions
   WHERE entity_tp = 'ORG')
  SELECT c.name, SUM(oc.transaction_amt)
  FROM committees c 
    LEFT OUTER JOIN ORGContribution oc ON c.id = oc.cmte_id
  GROUP BY c.id, c.name
;

-- Question 6
CREATE VIEW q6 (id) AS
  -- SELECT DISTINCT cc.cand_id
  -- FROM committee_contributions cc
  -- WHERE cc.entity_tp = 'PAC'
  -- INTERSECT
  -- SELECT DISTINCT cc.cand_id
  -- FROM committee_contributions cc 
  -- WHERE cc.entity_tp = 'CCM'

  SELECT DISTINCT cc1.cand_id
  FROM committee_contributions cc1
    INNER JOIN committee_contributions cc2 ON cc1.cand_id = cc2.cand_id
    AND cc1.entity_tp = 'PAC'
    AND cc2.entity_tp = 'CCM'
;

-- Question 7
CREATE VIEW q7 (cand_name1, cand_name2) AS
  WITH RIContributions(cand_id, cmte_id) AS
  (SELECT DISTINCT cand_id, cmte_id
   FROM committee_contributions
   WHERE state = 'RI')
  SELECT c1.name, c2.name
  FROM candidates c1, candidates c2, RIContributions cc1, RIContributions cc2
  WHERE cc1.cand_id = c1.id AND cc1.cmte_id = cc2.cmte_id AND cc2.cand_id = c2.id
  GROUP BY c1.id, c2.id
  HAVING c1.id != c2.id
;