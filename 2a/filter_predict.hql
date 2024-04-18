add file projects/2a/model.py;
add file projects/2a/predict.py;
add file 2a.joblib;
insert overwrite table hw2_pred
(select transform(*)
using '/opt/conda/envs/dsenv/bin/python3 predict.py'
from hw2_test
where ((if1 > 20) and (if1 < 40)));
