CREATE TABLE IF NOT EXISTS sdc_detail.ppe_scanlog(
    taskid string,
    ip string,
    iptype string,
    iphplace string,
    mac string,
    assettype string,
    osver string,
    port string,
    opstype string,
    softver string,
    httpwarever string,
    sfinfo string,
    number string,
    title string,
    serverity string,
    products string,
    isevent string,
    submittime string,
    opentime string,
    discoverername string,
    formalway string,
    description string,
    patchname string,
    patchdescription string,
    time string
)
PARTITIONED BY (day string, minute string)
rOW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS PARQUET
;
