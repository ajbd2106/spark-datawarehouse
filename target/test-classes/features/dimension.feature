Feature: dimension processing with scd1 and scd2 fields

  Scenario: handle dimension by copying all the table
  We suppose here, that name attribute is SCD1 and type attribute is SCD2
  in staging table :
  - id 3542  is a SCD2 and SCD1 change
  - id 3543 a SCD1 change
  - id 3544 is a new line
  - id 3547 is a SCD2 change only

  in current table :
  - tech id 1 is an inactive line that should be kept.
  - tech id 4 is an active line on which there are no changes, and it should be kept
  - tech id 5 is an inactive line that should not appear in lookup dimension

  As there is no default line inserted in current dimension data, a fake line should be created automatically.

    Given a bean with scd1 and scd2 fields
    And this current dimension data
      | id | functionalId | name  | type  | startDate  | endDate    | updatedDate | current | scd1Checksum                     | scd2Checksum                     |
      | 1  | 3542         | name1 | type0 | 2016-01-01 | 2016-12-31 | 2016-01-01  | N       | 836db9ecc83d8397a5d0205eb8344d4c | 828a71027955270c8a79ef5b8f570cde |
      | 2  | 3542         | name1 | type1 | 2017-01-01 | 2200-01-01 | 2017-01-01  | Y       | 836db9ecc83d8397a5d0205eb8344d4c | 3156e42ab24604b8de92a93ed761532d |
      | 3  | 3543         | name2 | type2 | 2017-01-01 | 2200-01-01 | 2017-01-01  | Y       | 5a7fcd4f1c785c8ef4931a5a9c698ac0 | 8fe8b170aa076a4233d8eda7d28804d4 |
      | 4  | 3545         | name5 | type2 | 2017-01-01 | 2200-01-01 | 2017-01-01  | Y       | 0de5fc94d0ba53fc7a44f0f136e82fbb | 8fe8b170aa076a4233d8eda7d28804d4 |
      | 5  | 3546         | name5 | type2 | 2017-01-01 | 2016-12-31 | 2017-01-01  | N       | 0de5fc94d0ba53fc7a44f0f136e82fbb | 8fe8b170aa076a4233d8eda7d28804d4 |
      | 6  | 3547         | name5 | type2 | 2017-01-01 | 2200-01-01 | 2017-01-01  | Y       | 0de5fc94d0ba53fc7a44f0f136e82fbb | 8fe8b170aa076a4233d8eda7d28804d4 |
    And staging data
      | functionalId | name  | type  |
      | 3542         | name1 | type2 |
      | 3543         | name3 | type2 |
      | 3544         | name4 | type1 |
      | 3547         | name5 | type1 |
    When i process dimension as of date "2017-12-15"
    Then i should obtain these old updated dimension lines
      | id | functionalId | name    | type    | startDate  | endDate    | updatedDate | current | scd1Checksum                     | scd2Checksum                     |
      | -1 | UNKNOWN      | UNKNOWN | UNKNOWN | 2017-12-15 | 2200-01-01 | CURRENTDATE | Y       | 696b031073e74bf2cb98e5ef201d4aa3 | 696b031073e74bf2cb98e5ef201d4aa3 |
      | 1  | 3542         | name1   | type0   | 2016-01-01 | 2016-12-31 | 2016-01-01  | N       | 836db9ecc83d8397a5d0205eb8344d4c | 828a71027955270c8a79ef5b8f570cde |
      | 2  | 3542         | name1   | type1   | 2017-01-01 | 2017-12-14 | CURRENTDATE | N       | 836db9ecc83d8397a5d0205eb8344d4c | 3156e42ab24604b8de92a93ed761532d |
      | 3  | 3543         | name3   | type2   | 2017-01-01 | 2200-01-01 | CURRENTDATE | Y       | 0e2bb4d743f2a009d4b84a9338c98f7c | 8fe8b170aa076a4233d8eda7d28804d4 |
      | 4  | 3545         | name5   | type2   | 2017-01-01 | 2200-01-01 | 2017-01-01  | Y       | 0de5fc94d0ba53fc7a44f0f136e82fbb | 8fe8b170aa076a4233d8eda7d28804d4 |
      | 5  | 3546         | name5   | type2   | 2017-01-01 | 2016-12-31 | 2017-01-01  | N       | 0de5fc94d0ba53fc7a44f0f136e82fbb | 8fe8b170aa076a4233d8eda7d28804d4 |
      | 6  | 3547         | name5   | type2   | 2017-01-01 | 2017-12-14 | CURRENTDATE | N       | 0de5fc94d0ba53fc7a44f0f136e82fbb | 8fe8b170aa076a4233d8eda7d28804d4 |
    And those new dimension lines
      | functionalId | name  | type  | startDate  | endDate    | updatedDate | current | scd1Checksum                     | scd2Checksum                     |
      | 3542         | name1 | type2 | 2017-12-15 | 2200-01-01 | CURRENTDATE | Y       | 836db9ecc83d8397a5d0205eb8344d4c | 8fe8b170aa076a4233d8eda7d28804d4 |
      | 3544         | name4 | type1 | 2017-12-15 | 2200-01-01 | CURRENTDATE | Y       | 3f6697692f4506cf311c95848f3536d3 | 3156e42ab24604b8de92a93ed761532d |
      | 3547         | name5 | type1 | 2017-12-15 | 2200-01-01 | CURRENTDATE | Y       | 0de5fc94d0ba53fc7a44f0f136e82fbb | 3156e42ab24604b8de92a93ed761532d |
    And dimension lines should have all distinct technical ids
    And dimension look up must contain
      | functionalId |
      | UNKNOWN      |
      | 3542         |
      | 3543         |
      | 3544         |
      | 3545         |
      | 3547         |

  Scenario: dimension processing with only scd1 fields
  Check that the process does not fail if there are no scd2 fields defined

    Given a bean with SCD1 only
    And this current dimension data
      | id | functionalId | name  | startDate  | endDate    | updatedDate | current | scd1Checksum                     |
      | 3  | 3543         | name2 | 2017-01-01 | 2200-01-01 | 2017-01-01  | Y       | 5a7fcd4f1c785c8ef4931a5a9c698ac0 |
    And staging data
      | functionalId | name  |
      | 3543         | name3 |
    When i process dimension as of date "2017-12-15"
    Then i should obtain these old updated dimension lines
      | id | functionalId | name    | startDate  | endDate    | updatedDate | current | scd1Checksum                     |
      | -1 | UNKNOWN      | UNKNOWN | 2017-12-15 | 2200-01-01 | CURRENTDATE | Y       | 696b031073e74bf2cb98e5ef201d4aa3 |
      | 3  | 3543         | name3   | 2017-01-01 | 2200-01-01 | CURRENTDATE | Y       | 0e2bb4d743f2a009d4b84a9338c98f7c |


  Scenario: dimension processing with only scd2 fields
  Check that the process does not fail if there are no scd1 fields defined

    Given a bean with SCD2 only
    And this current dimension data
      | id | functionalId | type  | startDate  | endDate    | updatedDate | current | scd2Checksum                     |
      | 1  | 3542         | type0 | 2016-01-01 | 2016-12-31 | 2016-01-01  | N       | 828a71027955270c8a79ef5b8f570cde |
      | 2  | 3542         | type1 | 2017-01-01 | 2200-01-01 | 2017-01-01  | Y       | 3156e42ab24604b8de92a93ed761532d |
    And staging data
      | functionalId | type  |
      | 3542         | type2 |
    When i process dimension as of date "2017-12-15"
    Then i should obtain these old updated dimension lines
      | id | functionalId | type    | startDate  | endDate    | updatedDate | current | scd2Checksum                     |
      | -1 | UNKNOWN      | UNKNOWN | 2017-12-15 | 2200-01-01 | CURRENTDATE | Y       | 696b031073e74bf2cb98e5ef201d4aa3 |
      | 1  | 3542         | type0   | 2016-01-01 | 2016-12-31 | 2016-01-01  | N       | 828a71027955270c8a79ef5b8f570cde |
      | 2  | 3542         | type1   | 2017-01-01 | 2017-12-14 | CURRENTDATE | N       | 3156e42ab24604b8de92a93ed761532d |
    And those new dimension lines
      | functionalId | type  | startDate  | endDate    | updatedDate | current | scd2Checksum                     |
      | 3542         | type2 | 2017-12-15 | 2200-01-01 | CURRENTDATE | Y       | 8fe8b170aa076a4233d8eda7d28804d4 |


  Scenario: no scd1 and no scd2 in the bean
  Check that the process does not fail if there are no scd1 and no scd2 fields defined

    Given a bean with no SCD1 and SCD2 fields
    And this current dimension data
      | id | functionalId | startDate  | endDate    | updatedDate | current |
      | 1  | 3541         | 2016-01-01 | 2016-12-31 | 2016-01-01  | N       |
      | 2  | 3542         | 2017-01-01 | 2200-01-01 | 2017-01-01  | Y       |
    And staging data
      | functionalId |
      | 3543         |
    When i process dimension as of date "2017-12-15"
    Then i should obtain these old updated dimension lines
      | id | functionalId | startDate  | endDate    | updatedDate | current |
      | -1 | UNKNOWN      | 2017-12-15 | 2200-01-01 | CURRENTDATE | Y       |
      | 1  | 3541         | 2016-01-01 | 2016-12-31 | 2016-01-01  | N       |
      | 2  | 3542         | 2017-01-01 | 2200-01-01 | 2017-01-01  | Y       |
    And those new dimension lines
      | functionalId | startDate  | endDate    | updatedDate | current |
      | 3543         | 2017-12-15 | 2200-01-01 | CURRENTDATE | Y       |

  Scenario: current table is empty

    Given a bean with scd1 and scd2 fields
    And this current dimension data
      | id | functionalId | name | type | startDate | endDate | updatedDate | current | scd1Checksum | scd2Checksum |
    And staging data
      | functionalId | name  | type  |
      | 3543         | name3 | type2 |
    When i process dimension as of date "2017-12-15"
    Then i should obtain these old updated dimension lines
      | id | functionalId | name    | type    | startDate  | endDate    | updatedDate | current | scd1Checksum                     | scd2Checksum                     |
      | -1 | UNKNOWN      | UNKNOWN | UNKNOWN | 2017-12-15 | 2200-01-01 | CURRENTDATE | Y       | 696b031073e74bf2cb98e5ef201d4aa3 | 696b031073e74bf2cb98e5ef201d4aa3 |
    And those new dimension lines
      | functionalId | name  | type  | startDate  | endDate    | updatedDate | current | scd1Checksum                     | scd2Checksum                     |
      | 3543         | name3 | type2 | 2017-12-15 | 2200-01-01 | CURRENTDATE | Y       | 0e2bb4d743f2a009d4b84a9338c98f7c | 8fe8b170aa076a4233d8eda7d28804d4 |

  Scenario: staging table is empty
  nothing should be done (no current table recopy)

    Given a bean with scd1 and scd2 fields
    And this current dimension data
      | id | functionalId | name  | type  | startDate  | endDate    | updatedDate | current | scd1Checksum                     | scd2Checksum                     |
      | 1  | 3542         | name1 | type0 | 2016-01-01 | 2016-12-31 | 2016-01-01  | N       | 836db9ecc83d8397a5d0205eb8344d4c | 828a71027955270c8a79ef5b8f570cde |
      | 2  | 3542         | name1 | type1 | 2017-01-01 | 2200-01-01 | 2017-01-01  | Y       | 836db9ecc83d8397a5d0205eb8344d4c | 3156e42ab24604b8de92a93ed761532d |
      | 3  | 3543         | name2 | type2 | 2017-01-01 | 2200-01-01 | 2017-01-01  | Y       | 5a7fcd4f1c785c8ef4931a5a9c698ac0 | 8fe8b170aa076a4233d8eda7d28804d4 |
      | 4  | 3545         | name5 | type2 | 2017-01-01 | 2200-01-01 | 2017-01-01  | Y       | 0de5fc94d0ba53fc7a44f0f136e82fbb | 8fe8b170aa076a4233d8eda7d28804d4 |
      | 5  | 3546         | name5 | type2 | 2017-01-01 | 2016-12-31 | 2017-01-01  | N       | 0de5fc94d0ba53fc7a44f0f136e82fbb | 8fe8b170aa076a4233d8eda7d28804d4 |
      | 6  | 3547         | name5 | type2 | 2017-01-01 | 2200-01-01 | 2017-01-01  | Y       | 0de5fc94d0ba53fc7a44f0f136e82fbb | 8fe8b170aa076a4233d8eda7d28804d4 |
    And staging data
      | functionalId | name | type |
    When i process dimension as of date "2017-12-15"
    Then no lines should be added to dimension

  Scenario: staging table is not empty, but there are no changes
  nothing should be done (no current table recopy)

    Given a bean with scd1 and scd2 fields
    And this current dimension data
      | id | functionalId | name  | type  | startDate  | endDate    | updatedDate | current | scd1Checksum                     | scd2Checksum                     |
      | 1  | 3542         | name1 | type0 | 2016-01-01 | 2016-12-31 | 2016-01-01  | N       | 836db9ecc83d8397a5d0205eb8344d4c | 828a71027955270c8a79ef5b8f570cde |
      | 2  | 3542         | name1 | type1 | 2017-01-01 | 2200-01-01 | 2017-01-01  | Y       | 836db9ecc83d8397a5d0205eb8344d4c | 3156e42ab24604b8de92a93ed761532d |
      | 3  | 3543         | name2 | type2 | 2017-01-01 | 2200-01-01 | 2017-01-01  | Y       | 5a7fcd4f1c785c8ef4931a5a9c698ac0 | 8fe8b170aa076a4233d8eda7d28804d4 |
      | 4  | 3545         | name5 | type2 | 2017-01-01 | 2200-01-01 | 2017-01-01  | Y       | 0de5fc94d0ba53fc7a44f0f136e82fbb | 8fe8b170aa076a4233d8eda7d28804d4 |
      | 5  | 3546         | name5 | type2 | 2017-01-01 | 2016-12-31 | 2017-01-01  | N       | 0de5fc94d0ba53fc7a44f0f136e82fbb | 8fe8b170aa076a4233d8eda7d28804d4 |
      | 6  | 3547         | name5 | type2 | 2017-01-01 | 2200-01-01 | 2017-01-01  | Y       | 0de5fc94d0ba53fc7a44f0f136e82fbb | 8fe8b170aa076a4233d8eda7d28804d4 |
    And staging data
      | functionalId | name  | type  |
      | 3542         | name1 | type1 |
      | 3543         | name2 | type2 |
      | 3545         | name5 | type2 |
    When i process dimension as of date "2017-12-15"
    Then no lines should be added to dimension

  Scenario: create fake line with Integer, Long, BigDecimal, String, Date as scd1 or scd2

    Given a bean with all type fields
    And this current dimension data
      | id | functionalId | scd1StringField | scd1IntegerField | scd1LongField | scd1BigDecimalField | scd1DateField | scd2StringField | scd2IntegerField | scd2LongField | scd2BigDecimalField | scd2DateField | startDate | endDate | updatedDate | current | scd1Checksum | scd2Checksum |
    And staging data
      | functionalId | scd1StringField | scd1IntegerField | scd1LongField | scd1BigDecimalField | scd1DateField | scd2StringField | scd2IntegerField | scd2LongField | scd2BigDecimalField | scd2DateField |
      | 3542         | scd1String      | 2                | 3             | 5.124               | 2017-12-21    | scd2String      | 6                | 9             | 1.548               | 2017-12-22    |
    When i process dimension as of date "2017-12-15"
    Then i should obtain these old updated dimension lines
      | id | functionalId | scd1StringField | scd1IntegerField | scd1LongField | scd1BigDecimalField | scd1DateField | scd2StringField | scd2IntegerField | scd2LongField | scd2BigDecimalField | scd2DateField | startDate  | endDate    | updatedDate | current | scd1Checksum                     | scd2Checksum                     |
      | -1 | UNKNOWN      | UNKNOWN         | 0                | 0             | 0.00                | 1970-01-01    | N/A             | -1               | -1            | -1                  | 1970-01-01    | 2017-12-15 | 2200-01-01 | CURRENTDATE | Y       | 4d278e40983c0beb6dc25b4bccd0a604 | 82bb3877fc5a8bed5f6e25aed93aa777 |
    And those new dimension lines
      | functionalId | scd1StringField | scd1IntegerField | scd1LongField | scd1BigDecimalField | scd1DateField | scd2StringField | scd2IntegerField | scd2LongField | scd2BigDecimalField | scd2DateField | startDate  | endDate    | updatedDate | current | scd1Checksum                     | scd2Checksum                     |
      | 3542         | scd1String      | 2                | 3             | 5.124               | 2017-12-21    | scd2String      | 6                | 9             | 1.548               | 2017-12-22    | 2017-12-15 | 2200-01-01 | CURRENTDATE | Y       | 2acc58fa627223a81f84dd5a135abc01 | 7fa8abcd1b944eb8302cb3dc15968435 |


#Scenario: update fake line if column has been added for instance.

#Scenario: no checksum columns, functional id, technical id, startdate, enddate, updatedDate, current defined. Should throw an exception