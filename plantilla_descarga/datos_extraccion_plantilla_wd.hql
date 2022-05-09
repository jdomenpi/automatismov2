select
regexp_replace (emplid , '\;|\t|,|\n|\r', ' '),
regexp_replace (id_wkd , '\;|\t|,|\n|\r', ' '),
regexp_replace (loa_type , '\;|\t|,|\n|\r', ' '),
regexp_replace (loa_reason , '\;|\t|,|\n|\r', ' '),
regexp_replace (start_dt , '\;|\t|,|\n|\r', ' '),
regexp_replace (end_dt , '\;|\t|,|\n|\r', ' '),
regexp_replace (estimated_end_dt , '\;|\t|,|\n|\r', ' '),
regexp_replace (last_day_work , '\;|\t|,|\n|\r', ' '),
regexp_replace (first_day_work , '\;|\t|,|\n|\r', ' '),
regexp_replace (leave_perc , '\;|\t|,|\n|\r', ' '),
regexp_replace (child_birthdate , '\;|\t|,|\n|\r', ' '),
regexp_replace (n_child , '\;|\t|,|\n|\r', ' '),
data_date_part
from ${hiveconf:PARAM_BU_WORKDAY}.datos_pklowercase where data_date_part='${hiveconf:FECHA}';