SELECT proname FROM pg_proc WHERE proname LIKE 'get_sp_otb%';

SELECT p.proname, pg_catalog.pg_get_function_arguments(p.oid) as arguments FROM pg_proc p JOIN pg_namespace n On n.oid = p.pronamespace WHERE p.proname LIKE 'get_sp_otb%'; 