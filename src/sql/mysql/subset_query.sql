-- Return a single row containing the value 'True' if either the rows in A are a subset of those in B, or vice 
-- versa (i.e. one table 'contains' the other); otherwise return an empty result set.
-- For simplicity, we assume A and B are identically structured:
-- create table (A|B) ( x, y );
--
-- To understand how this comes about, realize that is we have two sets X and Y, then asking whether X subset 
-- Y or Y subset X is equivalent to tasking whether |X intersect Y| = min(|X|, |Y|)
select 'True' from (select 1) t where ( 
    select count(*) from (
        select * from A join B using (x, y)
    ) tmp ) = least(( select count(*) from A ), ( select count(*) from B ))
);
