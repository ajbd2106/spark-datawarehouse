Feature: fact processing

  Scenario: handle junk dimensions
  fact table process should first handle junk dimension data.
  -> if some combinations does not exist in the junk dimension, new lines must be created in both lookup and dimension tables.

    Given this current lookup dimension data
      | id | functionalId |
      | -1 | UNKNOWN      |
      | 3  | 3543         |
      | 4  | 3545         |
      | 5  | 3544         |
      | 6  | 3542         |
    And this current junk dimension data
      | id | field1      | updatedDate | checksum                         |
      | 1  | field1Value | 2017-12-03  | bc23176e41899331b9f9a1c73527e5b8 |
    And this current junk lookup dimension data
      | id | functionalId                     |
      | 1  | bc23176e41899331b9f9a1c73527e5b8 |
    And this current fact data
      | partyRole1FuncId | partyRole2FuncId | amount1      | amount2     | junkDimension1.field1 | junkDimension2.field1 |
      | 3543             | 3543             | 4586123.4512 | 4512345.845 | field1Value           | field3Value           |
      | 3542             | 3544             | 7845123.4512 | 6547864.845 | field2Value           | field2Value           |
      | 3541             | 3545             | 9845641.4512 | 7845312.845 | field1Value           | field1Value           |
    When i process fact with date as of "2017-12-15"
    Then i should add following junk dimension data
      | field1      | updatedDate | checksum                         |
      | field3Value | 2017-12-15  | 56b5dcd844be1b79993635897cd62573 |
      | field2Value | 2017-12-15  | 834d7283bb6217f9f104c91c800781c1 |
    And i should add following junk dimension lookup data
      | functionalId                     |
      | 56b5dcd844be1b79993635897cd62573 |
      | 834d7283bb6217f9f104c91c800781c1 |

  Scenario: handle fact table data
  a join must be done for each dimension, replacing the functional id by the technical id.
  -> if the key is not found in the dimension table (which is not possible for the junk dimension), line must be attached to a fake line that own each dimension table.

    Given this current lookup dimension data
      | id | functionalId |
      | -1 | UNKNOWN      |
      | 3  | 3543         |
      | 4  | 3545         |
      | 5  | 3544         |
      | 6  | 3542         |
    And this current junk dimension data
      | id | field1      | updatedDate | checksum                         |
      | 1  | field1Value | 2017-12-03  | bc23176e41899331b9f9a1c73527e5b8 |
      | 2  | field2Value | 2017-12-14  | 834d7283bb6217f9f104c91c800781c1 |
      | 3  | field3Value | 2017-12-14  | 56b5dcd844be1b79993635897cd62573 |
    And this current junk lookup dimension data
      | id | functionalId                     |
      | 1  | bc23176e41899331b9f9a1c73527e5b8 |
      | 2  | 834d7283bb6217f9f104c91c800781c1 |
      | 3  | 56b5dcd844be1b79993635897cd62573 |
    And this current fact data
      | partyRole1FuncId | partyRole2FuncId | amount1      | amount2     | junkDimension1.field1 | junkDimension2.field1 |
      | 3543             | 3543             | 4586123.4512 | 4512345.845 | field1Value           | field3Value           |
      | 3542             | 3544             | 7845123.4512 | 6547864.845 | field2Value           | field2Value           |
      | 3541             | 3545             | 9845641.4512 | 7845312.845 | field1Value           | field1Value           |
    When i process fact with date as of "2017-12-15"
    Then no lines should be added to junk dimension data
    And no lines should be added to junk lookup dimension data
    And i should get this fact table data
      | inventoryDate | partyRole1Id | partyRole2Id | junkDimension1Id | junkDimension2Id | amount1        | amount2        |
      | 20171215      | -1           | 4            | 1                | 1                | 9845641.451200 | 7845312.845000 |
      | 20171215      | 3            | 3            | 1                | 3                | 4586123.451200 | 4512345.845000 |
      | 20171215      | 6            | 5            | 2                | 2                | 7845123.451200 | 6547864.845000 |


  Scenario: handle flattened fact table
  It's pretty much the same scenario than normal fact table. The main difference is that this option copy technical id, functional id, and properties columns in the fact table,
  instead of keeping them in the dimension. This is mainly used for hive as it can have a columnar format as storage backend.
  If it has several roles, each column that contain property of a role must be prefixed with the role name and a _ as a separator.

    Given current dimension data
      | id | functionalId | name    | type    | startDate  | endDate    | updatedDate | current | scd1Checksum                     | scd2Checksum                     |
      | -1 | UNKNOWN      | UNKNOWN | UNKNOWN | 2017-12-15 | 2100-01-01 | 2017-12-15  | Y       | 696b031073e74bf2cb98e5ef201d4aa3 | 696b031073e74bf2cb98e5ef201d4aa3 |
      | 1  | 3542         | name1   | type0   | 2016-01-01 | 2016-12-31 | 2016-01-01  | N       | 836db9ecc83d8397a5d0205eb8344d4c | 828a71027955270c8a79ef5b8f570cde |
      | 2  | 3542         | name1   | type1   | 2017-01-01 | 2017-12-14 | 2017-01-01  | N       | 836db9ecc83d8397a5d0205eb8344d4c | 3156e42ab24604b8de92a93ed761532d |
      | 3  | 3543         | name3   | type2   | 2017-01-01 | 2100-01-01 | 2017-12-15  | Y       | 0e2bb4d743f2a009d4b84a9338c98f7c | 8fe8b170aa076a4233d8eda7d28804d4 |
      | 4  | 3545         | name5   | type2   | 2017-01-01 | 2100-01-01 | 2017-01-01  | Y       | 0de5fc94d0ba53fc7a44f0f136e82fbb | 8fe8b170aa076a4233d8eda7d28804d4 |
      | 5  | 3544         | name4   | type1   | 2017-12-15 | 2100-01-01 | 2017-12-15  | Y       | 3f6697692f4506cf311c95848f3536d3 | 3156e42ab24604b8de92a93ed761532d |
      | 6  | 3542         | name1   | type2   | 2017-12-15 | 2100-01-01 | 2017-12-15  | Y       | 836db9ecc83d8397a5d0205eb8344d4c | 8fe8b170aa076a4233d8eda7d28804d4 |
    And this current junk dimension data
      | id | field1      | updatedDate | checksum                         |
      | 1  | field1Value | 2017-12-03  | bc23176e41899331b9f9a1c73527e5b8 |
      | 2  | field2Value | 2017-12-14  | 834d7283bb6217f9f104c91c800781c1 |
      | 3  | field3Value | 2017-12-14  | 56b5dcd844be1b79993635897cd62573 |
    And this current junk lookup dimension data
      | id | functionalId                     |
      | 1  | bc23176e41899331b9f9a1c73527e5b8 |
      | 2  | 834d7283bb6217f9f104c91c800781c1 |
      | 3  | 56b5dcd844be1b79993635897cd62573 |
    And this current fact data
      | partyRole1FuncId | partyRole2FuncId | amount1      | amount2     | junkDimension1.field1 | junkDimension2.field1 |
      | 3543             | 3543             | 4586123.4512 | 4512345.845 | field1Value           | field3Value           |
      | 3542             | 3544             | 7845123.4512 | 6547864.845 | field2Value           | field2Value           |
      | 3541             | 3545             | 9845641.4512 | 7845312.845 | field1Value           | field1Value           |
    When i process fact with date as of "2017-12-15" and flattenize option
    Then i should get this flattened fact table data
      | inventoryDate | primaryBorrower_id | primaryBorrower_functionalId | primaryBorrower_name | primaryBorrower_type | obligor_id | obligor_functionalId | obligor_name | obligor_type | amount1        | amount2        | junkDimension1_id | junkDimension1_field1 | junkDimension2_id | junkDimension2_field1 |
      | 20171215      | 3                  | 3543                         | name3                | type2                | 3          | 3543                 | name3        | type2        | 4586123.451200 | 4512345.845000 | 1                 | field1Value           | 3                 | field3Value           |
      | 20171215      | 6                  | 3542                         | name1                | type2                | 5          | 3544                 | name4        | type1        | 7845123.451200 | 6547864.845000 | 2                 | field2Value           | 2                 | field2Value           |
      | 20171215      | -1                 | UNKNOWN                      | UNKNOWN              | UNKNOWN              | 4          | 3545                 | name5        | type2        | 9845641.451200 | 7845312.845000 | 1                 | field1Value           | 1                 | field1Value           |