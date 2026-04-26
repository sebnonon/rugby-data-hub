-- Backfill score_rec, score_adv, adversaire_nom_complet sur les données démo existantes
UPDATE match SET
  adversaire_nom_complet = v.nom,
  score_rec = v.sr,
  score_adv = v.sa
FROM (VALUES
  (1,  'Ol Marcquois Rugby',           28, 23),
  (2,  'CS Bourgoin Jallieu',          40, 29),
  (3,  'Niort Rugby Club',             32, 10),
  (4,  'RC Narbonne',                  37, 14),
  (5,  'SC Albi',                      18, 32),
  (6,  'Stado Tarbes Pyrenees Rugby',  31, 28),
  (7,  'Stade Niçois',                 29, 23),
  (8,  'Rouen Normandie',              20, 27),
  (9,  'CA Perigourdin',               21,  8),
  (10, 'RC Massy Essonne',             14, 15),
  (11, 'SO Chambery Rugby',            15, 20),
  (12, 'RC Suresnes Hauts De Seine',   22, 23),
  (13, 'US Bressane',                  19, 12),
  (14, 'ASM',                          14, 18),
  (15, 'BOR',                          31, 10),
  (16, 'PAU',                          28, 25),
  (17, 'DAO',                          23, 19),
  (18, 'LYO',                          36, 21),
  (19, 'TLS',                          31, 30),
  (20, 'MHR',                          41, 19),
  (21, 'Ol Marcquois Rugby',           22, 18),
  (22, 'CS Bourgoin Jallieu',          23, 10)
) AS v(match_id, nom, sr, sa)
WHERE match.match_id = v.match_id;
