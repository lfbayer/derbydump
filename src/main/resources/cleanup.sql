CREATE FUNCTION fixup_sequences() RETURNS void AS $$
  DECLARE
    target_seq RECORD;
    max_result RECORD;
  BEGIN
    FOR target_seq IN select s.relname as seq, n.nspname as sch, t.relname as tab, a.attname as col
          from pg_class s
          join pg_depend d on d.objid=s.oid and d.classid='pg_class'::regclass and d.refclassid='pg_class'::regclass
          join pg_class t on t.oid=d.refobjid
          join pg_namespace n on n.oid=t.relnamespace
          join pg_attribute a on a.attrelid=t.oid and a.attnum=d.refobjsubid
          where s.relkind='S' and d.deptype='a'
    LOOP
      FOR max_result IN EXECUTE 'SELECT cast((coalesce(MAX(' || quote_ident(target_seq.col) || '), 0)+1) as BIGINT) as last FROM ' || quote_ident(target_seq.tab)
      LOOP
        RAISE NOTICE 'new sequence value for column %.%: %', target_seq.tab, target_seq.col, max_result.last;
        EXECUTE 'SELECT setval(''' || quote_ident(target_seq.seq) || ''', ' || max_result.last || ', false)';
      END LOOP;
    END LOOP;
  END;
$$ LANGUAGE plpgsql;
SELECT fixup_sequences();
DROP FUNCTION fixup_sequences();

CREATE FUNCTION fix_up_manual_sequences() RETURNS void AS $$
  DECLARE
    target_seq RECORD;
    max_result RECORD;
  BEGIN
    FOR target_seq IN SELECT s.relname as seq, substr(s.relname, 0, length(s.relname) - 3) AS tab
      FROM pg_class s
      LEFT JOIN pg_depend d on d.objid=s.oid and d.classid='pg_class'::regclass and d.refclassid='pg_class'::regclass
      WHERE s.relkind='S' AND d.objid IS NULL
    LOOP
      FOR max_result IN EXECUTE 'SELECT cast((coalesce(MAX(id), 0)+1) as BIGINT) as last FROM ' || quote_ident(target_seq.tab)
      LOOP
        RAISE NOTICE 'new sequence value for column %.id: %', target_seq.tab, max_result.last;
        EXECUTE 'SELECT setval(''' || quote_ident(target_seq.seq) || ''', ' || max_result.last || ', false)';
      END LOOP;
    END LOOP;
  END;
$$ LANGUAGE plpgsql;
SELECT fix_up_manual_sequences();
DROP FUNCTION fix_up_manual_sequences();
