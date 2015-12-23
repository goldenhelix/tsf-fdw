
-- Join reseq_1 and refseq_2 with the ID mapping in refseq_3 (sucha s
-- when these are created by the following command)
-- python3 example_usage.py test_tsf_fdw ~/Documents/Projects/Tumor-Normal\ Template/data/VarsOfTumorNormal_RefSeqGenes105v2-NCBI.tsf refseq
SELECT l.*, r.*, j.*
FROM refseq_1 l, refseq_2 r, refseq_3 j
WHERE l._id = j._id AND r._id = ANY(j.record_mapping_) LIMIT 10;

-- Query a 1:M (one-to-many) table refseq_2 based on a left-hand record
-- of ? through a join table refseq_3
SELECT t.*
FROM refseq_2 t, refseq_3 j
WHERE j._id = ? AND t._id = ANY(j.record_mapping_);

-- Generates inner queries that utalize the ReScan FDW function
SELECT cardio_1._id FROM cardio_1
INNER JOIN caridio_refseq_1 ON (cardio_1._id = caridio_refseq_1._id) 
WHERE 
( 
   caridio_refseq_1."GeneNames" @> ARRAY['APOB']::text[]  AND 
   caridio_refseq_1."EffectCombined" IN ('Missense', 'LoF') 
);
