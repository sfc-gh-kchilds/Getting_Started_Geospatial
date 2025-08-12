----------------------------------------------------------
---------------Part 1 Validation NY Taxi Rides------------
----------------------------------------------------------
use role ACCOUNTADMIN;
use warehouse my_wh;
select util_db.public.se_grader(step, (actual = expected),
    actual, expected, description) as graded_results from
    (SELECT
    'SEDW02' as step
        ,( select count(*) from
        advanced_analytics.information_schema.TABLE s where
        table_name in ('NY_TAXI_RIDES',
            'NY_TAXI_RIDES_H3','NY_TAXI_RIDES_H3_TRAIN','NY_TAXI_RIDES_H3_PREDICT','NY_TAXI_RIDES_COMPARE','NY_TAXI_RIDES_METRICS')) as actual
, 6 as expected
,'GEOSPATIAL TABLES CREATED SUCCESSFULLY!' as description

----------------------------------------------------------
-------------------Part 2 Order Reviews-------------------
----------------------------------------------------------
use role ACCOUNTADMIN;
use warehouse my_wh;
select util_db.public.se_grader(step, (actual = expected),
    actual, expected, description) as graded_results from
    (SELECT
    'SEDW03' as step
        ,( select count(*) from
        advanced_analytics.information_schema.TABLE s where
        table_name in ('ORDERS_REVIEWS',
            'ORDERS_REVIEWS_SENTIMENT_TEST','ORDERS_REVIEWS_SENTIMENT',
            'ORDERS_REVIEWS_SENTIMENT_ANALYSIS')) as actual
    , 4 as expected
    ,'GEOSPATIAL ORDERS TABLES CREATED SUCCESSFULLY!' as
    description
    );
