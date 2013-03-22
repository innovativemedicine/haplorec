-- use this schema to test the practicality of doing large queries based on 
-- a subset x where x subset s1.s or s1.s subset x (which will happen when we 
-- want to map (DrugName, { (GeneName, PhenotypeName) }) -> Recommendation 
-- where x = { (GeneName, PhenotypeName) } derived from mapping snps -> 
-- haplotypes -> { (GeneName, PhenotypeName) }, and s1.s = { (GeneName, PhenotypeName) } in recommendation table
CREATE TABLE s1 (
    s set(integer)
);

CREATE TABLE s2 (
    s set(integer)
);
