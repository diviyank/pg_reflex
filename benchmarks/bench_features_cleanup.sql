-- Tear down the bench_features_matrix.sql fixtures.
\set ON_ERROR_STOP off
DROP FUNCTION IF EXISTS bench_run(text,text,text,text,text);
DROP TABLE IF EXISTS s_ptnp, s_ptl, s_agnp, s_agl, s_cte, s_ctel,
                     s_anch, s_sec, s_anchp, s_secp, s_agr, s_pti CASCADE;
