CREATE TABLE oura_sleep (
    
    /*Sleep events are uniquely identified by period ID per summary date*/
    
    CONSTRAINT sleep_id     PRIMARY KEY (summary_date, period_id),

    awake               INTEGER,
    bedtime_end         TIMESTAMPTZ,
    bedtime_end_delta       INTEGER,
    bedtime_start           TIMESTAMPTZ,
    bedtime_start_delta     INTEGER,
    breath_average          FLOAT,
    deep                INTEGER,
    duration            INTEGER,
    efficiency          SMALLINT,
    hr_5min             SMALLINT[],
    hr_average          FLOAT,
    hr_lowest           SMALLINT,
    hypnogram_5min          CHAR[],
    is_longest          BOOLEAN,
    light               INTEGER,
    midpoint_at_delta       INTEGER,
    midpoint_time           INTEGER,
    onset_latency           INTEGER,
    period_id           SMALLINT,
    rem             INTEGER,
    restless            SMALLINT,
    rmssd               SMALLINT,
    rmssd_5min          SMALLINT[],
    score               SMALLINT,
    score_alignment         SMALLINT,
    score_deep          SMALLINT,
    score_disturbances      SMALLINT,
    score_efficiency        SMALLINT,
    score_latency           SMALLINT,
    score_rem           SMALLINT,
    score_total         SMALLINT,
    summary_date            DATE,
    temperature_delta       FLOAT,
    temperature_deviation       FLOAT,
    temperature_trend_deviation FLOAT,
    timezone            SMALLINT,
    total               INTEGER
);

CREATE TABLE oura_activity (
    summary_date DATE PRIMARY KEY,
    
    cal_active          SMALLINT,
    cal_total           SMALLINT,
    daily_movement          SMALLINT,
    day_end             TIMESTAMPTZ,
    day_start           TIMESTAMPTZ,
    high                SMALLINT,
    inactive            SMALLINT,
    inactivity_alerts       SMALLINT,
    low             SMALLINT,
    medium              SMALLINT,
    met_min_inactive        SMALLINT,
    non_wear            SMALLINT,
    rest                SMALLINT,
    score               SMALLINT,
    steps               INTEGER,
    timezone            SMALLINT,
    average_met         FLOAT,
    class_5min          SMALLINT[],
    met_1min            FLOAT[],
    met_min_high            SMALLINT,
    met_min_low         SMALLINT,
    met_min_medium          SMALLINT,
    met_min_medium_plus     SMALLINT,
    score_meet_daily_targets    SMALLINT,
    score_move_every_hour       SMALLINT,
    score_recovery_time     SMALLINT,
    score_stay_active       SMALLINT,
    score_training_frequency    SMALLINT,
    score_training_volume       SMALLINT,
    target_calories         SMALLINT,
    target_km           SMALLINT,
    target_miles            SMALLINT,
    to_target_km            FLOAT,
    to_target_miles         FLOAT,
    total               SMALLINT
);

CREATE TABLE oura_readiness (
    
    /*Readiness has only one row per sleep event*/
    
    CONSTRAINT readiness_id PRIMARY KEY (summary_date, period_id),

    period_id       SMALLINT,
    score_activity_balance  SMALLINT,
    score_previous_day  SMALLINT,
    score_previous_night    SMALLINT,
    score_recovery_index    SMALLINT,
    score_resting_hr    SMALLINT,
    score_sleep_balance SMALLINT,
    score_temperature   SMALLINT,
    summary_date        DATE,
    score           SMALLINT
);