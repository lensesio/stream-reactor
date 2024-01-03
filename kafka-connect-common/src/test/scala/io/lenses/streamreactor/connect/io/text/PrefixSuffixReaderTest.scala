/*
 * Copyright 2017-2024 Lenses.io Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.lenses.streamreactor.connect.io.text

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.io.ByteArrayInputStream
import java.io.InputStream

class PrefixSuffixReaderTest extends AnyFunSuite with Matchers {
  test("empty input stream returns None") {
    val reader = new PrefixSuffixReader(createInputStream(""), "prefix", "suffix")
    reader.next() should be(None)
  }
  test("prefix and suffix as one line") {
    val reader = new PrefixSuffixReader(createInputStream("prefixvalue1suffix"), "prefix", "suffix")
    reader.next() should be(Some("prefixvalue1suffix"))
    reader.next() should be(None)
  }
  test("prefix and suffix as two lines") {
    val reader = new PrefixSuffixReader(createInputStream("prefixvalue1\nsuffix"), "prefix", "suffix")
    reader.next() should be(Some("prefixvalue1\nsuffix"))
    reader.next() should be(None)
  }
  test("prefix and suffix as three lines") {
    val reader =
      new PrefixSuffixReader(createInputStream("prefixvalue1\nvalue2\nsuffix"), "prefix", "suffix")
    reader.next() should be(Some("prefixvalue1\nvalue2\nsuffix"))
    reader.next() should be(None)
  }

  test("multiple records on the same line") {
    val reader =
      new PrefixSuffixReader(createInputStream("prefixvalue1suffixprefixvalue2suffix"), "prefix", "suffix")
    reader.next() should be(Some("prefixvalue1suffix"))
    reader.next() should be(Some("prefixvalue2suffix"))
    reader.next() should be(None)
  }
  test("multiple records on the same line and multiple lines") {
    val reader =
      new PrefixSuffixReader(createInputStream("prefixvalue1suffixprefixvalue2suffix\nprefixvalue3suffix"),
                             "prefix",
                             "suffix",
      )
    reader.next() should be(Some("prefixvalue1suffix"))
    reader.next() should be(Some("prefixvalue2suffix"))
    reader.next() should be(Some("prefixvalue3suffix"))
    reader.next() should be(None)
  }
  test("multiple records on the same line, last record spanning to the next line") {
    val reader =
      new PrefixSuffixReader(createInputStream("prefixvalue1suffix\nprefixvalue2suffixprefix\nvalue3suffix"),
                             "prefix",
                             "suffix",
      )
    reader.next() should be(Some("prefixvalue1suffix"))
    reader.next() should be(Some("prefixvalue2suffix"))
    reader.next() should be(Some("prefix\nvalue3suffix"))
    reader.next() should be(None)
  }
  test(
    "multiple records on the same line, last record spanning to the next 2 lines, then more records on the same line",
  ) {
    val reader = new PrefixSuffixReader(
      createInputStream("prefixvalue1suffix\nprefixvalue2suffixprefix\nvalue3suffixprefixvalue4suffix"),
      "prefix",
      "suffix",
    )
    reader.next() should be(Some("prefixvalue1suffix"))
    reader.next() should be(Some("prefixvalue2suffix"))
    reader.next() should be(Some("prefix\nvalue3suffix"))
    reader.next() should be(Some("prefixvalue4suffix"))
    reader.next() should be(None)
  }
  test("prefix found but not suffix over multiple lines input") {
    val reader =
      new PrefixSuffixReader(createInputStream("prefixvalue1\nvalue2\nvalue3"), "prefix", "suffix")
    reader.next() should be(None)
  }
  test("prefix found but not suffix over multiple lines input with lines skipped") {
    val reader =
      new PrefixSuffixReader(createInputStream("value0\nprefixvalue1\nvalue2\nvalue3"), "prefix", "suffix")
    reader.next() should be(None)
  }
  test("record returned before no suffix is found") {
    val reader =
      new PrefixSuffixReader(createInputStream("prefixvalue1\nvalue2\nvalue3\nsuffixprefixvalue4"), "prefix", "suffix")
    reader.next() should be(Some("prefixvalue1\nvalue2\nvalue3\nsuffix"))
    reader.next() should be(None)
  }

  test("record returned before the content has suffix, and then another record is returned") {
    val reader = new PrefixSuffixReader(createInputStream(
                                          """
                                            |prefixvalue1
                                            |value2
                                            |value3
                                            |prefixvalue4
                                            |suffix
                                            |value5suffix
                                            |prefixvalue6suffix""".stripMargin,
                                        ),
                                        "prefix",
                                        "suffix",
    )
    reader.next() should be(Some("prefixvalue1\nvalue2\nvalue3\nprefixvalue4\nsuffix"))
    reader.next() should be(Some("prefixvalue6suffix"))
    reader.next() should be(None)
  }

  test("handle Instructor xml tags") {
    val reader =
      new PrefixSuffixReader(createInputStream(
                               """"
                                 |<?xml version="1.0" encoding="utf-8"?>
                                 |<InstructorPayLevel>
                                 |    <Instructor>
                                 |        <EmployeeNumber>188</EmployeeNumber>
                                 |        <PayLevel>11</PayLevel>
                                 |        <BidPeriod>22-06</BidPeriod>
                                 |        <PayLevelDescription>Description1</PayLevelDescription>
                                 |        <TrainingInstructorCategory>Category1</TrainingInstructorCategory>
                                 |        <DataSource>PR</DataSource>
                                 |    </Instructor>
                                 |    <Instructor>
                                 |        <EmployeeNumber>173</EmployeeNumber>
                                 |        <PayLevel>11</PayLevel>
                                 |        <BidPeriod>22-06</BidPeriod>
                                 |        <PayLevelDescription>1485$ level  APD</PayLevelDescription>
                                 |        <TrainingInstructorCategory>Category2</TrainingInstructorCategory>
                                 |        <DataSource>PR</DataSource>
                                 |    </Instructor>
                                 | </InstructorPayLevel>
                                 |""".stripMargin,
                             ),
                             "<Instructor>",
                             "</Instructor>",
      )
    reader.next() shouldBe Some(
      """<Instructor>
        |        <EmployeeNumber>188</EmployeeNumber>
        |        <PayLevel>11</PayLevel>
        |        <BidPeriod>22-06</BidPeriod>
        |        <PayLevelDescription>Description1</PayLevelDescription>
        |        <TrainingInstructorCategory>Category1</TrainingInstructorCategory>
        |        <DataSource>PR</DataSource>
        |    </Instructor>""".stripMargin,
    )
    reader.next() shouldBe Some(
      """<Instructor>
        |        <EmployeeNumber>173</EmployeeNumber>
        |        <PayLevel>11</PayLevel>
        |        <BidPeriod>22-06</BidPeriod>
        |        <PayLevelDescription>1485$ level  APD</PayLevelDescription>
        |        <TrainingInstructorCategory>Category2</TrainingInstructorCategory>
        |        <DataSource>PR</DataSource>
        |    </Instructor>""".stripMargin,
    )
    reader.next() shouldBe None
  }
  test("handle Instructor xml tags while trimming content is enabled") {
    val reader =
      new PrefixSuffixReader(createInputStream(
                               """"
                                 |<?xml version="1.0" encoding="utf-8"?>
                                 |<InstructorPayLevel>
                                 |    <Instructor>
                                 |        <EmployeeNumber>188</EmployeeNumber>
                                 |        <PayLevel>11</PayLevel>
                                 |        <BidPeriod>22-06</BidPeriod>
                                 |        <PayLevelDescription>Description1</PayLevelDescription>
                                 |        <TrainingInstructorCategory>Category1</TrainingInstructorCategory>
                                 |        <DataSource>PR</DataSource>
                                 |    </Instructor>
                                 |    <Instructor>
                                 |        <EmployeeNumber>173</EmployeeNumber>
                                 |        <PayLevel>11</PayLevel>
                                 |        <BidPeriod>22-06</BidPeriod>
                                 |        <PayLevelDescription>1485$ level  APD</PayLevelDescription>
                                 |        <TrainingInstructorCategory>Category2</TrainingInstructorCategory>
                                 |        <DataSource>PR</DataSource>
                                 |    </Instructor>
                                 | </InstructorPayLevel>
                                 |""".stripMargin,
                             ),
                             "<Instructor>",
                             "</Instructor>",
      )

    reader.next() shouldBe Some(
      """<Instructor>
        |        <EmployeeNumber>188</EmployeeNumber>
        |        <PayLevel>11</PayLevel>
        |        <BidPeriod>22-06</BidPeriod>
        |        <PayLevelDescription>Description1</PayLevelDescription>
        |        <TrainingInstructorCategory>Category1</TrainingInstructorCategory>
        |        <DataSource>PR</DataSource>
        |    </Instructor>""".stripMargin,
    )
    reader.next() shouldBe Some(
      """<Instructor>
        |        <EmployeeNumber>173</EmployeeNumber>
        |        <PayLevel>11</PayLevel>
        |        <BidPeriod>22-06</BidPeriod>
        |        <PayLevelDescription>1485$ level  APD</PayLevelDescription>
        |        <TrainingInstructorCategory>Category2</TrainingInstructorCategory>
        |        <DataSource>PR</DataSource>
        |    </Instructor>""".stripMargin,
    )
  }

  test("employee list xml tags") {
    val xml =
      """
        |<?xml version="1.0" encoding="utf-8"?>
        |<A>
        |    <Bid>202206</Bid>
        |    <CreatedTimeStamp>2022-04-07T12:23:07.5034062-05:00</CreatedTimeStamp>
        |    <CreatedBy>ME</CreatedBy>
        |    <Carrier>FlyFly</Carrier>
        |    <EmployeeList>
        |        <Employee><EmployeeNumber>1</EmployeeNumber><FirstName>Joseph</FirstName><LastName>Smith</LastName><MiddleName>L</MiddleName><EmployeeStatus>Active</EmployeeStatus><EmployeeCategory>Line Pilot</EmployeeCategory><PilotCommentID>0</PilotCommentID><BaseCode>IAH</BaseCode><PreviousBaseCode>IAH</PreviousBaseCode><BaseSeniorityNumber>82</BaseSeniorityNumber><SystemSeniorityNumber>1a</SystemSeniorityNumber><StandardizedSeniorityNumber>1a</StandardizedSeniorityNumber><MasterSeniorityNumber>1a</MasterSeniorityNumber><ActiveSeniorityNumber>1a</ActiveSeniorityNumber><SeniorityDate>2022-05-29T12:00:29.0000000-05:00</SeniorityDate><EquipmentCode>737</EquipmentCode><PreviousEquipmentCode>737</PreviousEquipmentCode><PositionCode>CA</PositionCode><PreviousPositionCode>CA</PreviousPositionCode><EffectiveDate>2022-06-30T12:00:30.0000000-05:00</EffectiveDate><EffectiveEndDate>2022-07-29T11:59:29.0000000-05:00</EffectiveEndDate><ChangeType /><AssignmentType /><AssignmentTypeExternalName>Assignment based upon the SBA</AssignmentTypeExternalName><Absences /></Employee>
        |        <Employee><EmployeeNumber>1</EmployeeNumber><FirstName>Paul</FirstName><LastName>Wood</LastName><MiddleName>F</MiddleName><EmployeeStatus>Active</EmployeeStatus><EmployeeCategory>Line Pilot</EmployeeCategory><PilotCommentID>0</PilotCommentID><BaseCode>EWR</BaseCode><PreviousBaseCode>EWR</PreviousBaseCode><BaseSeniorityNumber>6</BaseSeniorityNumber><SystemSeniorityNumber>2b</SystemSeniorityNumber><StandardizedSeniorityNumber>2b</StandardizedSeniorityNumber><MasterSeniorityNumber>2b</MasterSeniorityNumber><ActiveSeniorityNumber>2b</ActiveSeniorityNumber><SeniorityDate>2021-03-10T12:00:10.0000000-05:00</SeniorityDate><EquipmentCode>756</EquipmentCode><PreviousEquipmentCode>756</PreviousEquipmentCode><PositionCode>CA</PositionCode><PreviousPositionCode>CA</PreviousPositionCode><EffectiveDate>2022-06-30T12:00:30.0000000-05:00</EffectiveDate><EffectiveEndDate>2022-07-29T11:59:29.0000000-05:00</EffectiveEndDate><ChangeType /><AssignmentType /><AssignmentTypeExternalName>Assignment based upon the SBA</AssignmentTypeExternalName><Absences><Absence><AbsenceType>ULA</AbsenceType><AbsenceStartDate>2022-11-20T12:00:20.0000000-06:00</AbsenceStartDate><AbsenceEndDate>2026-11-31T12:00:31.0000000-06:00</AbsenceEndDate></Absence><Absence><AbsenceType>VAC</AbsenceType><AbsenceStartDate>2022-06-25T12:00:26.0000000-05:00</AbsenceStartDate><AbsenceEndDate>2022-07-12T12:00:02.0000000-05:00</AbsenceEndDate></Absence></Absences></Employee>
        |    </EmployeeList>
        |</A>
        |""".stripMargin

    val reader = new PrefixSuffixReader(createInputStream(xml), "<Employee>", "</Employee>")
    reader.next() shouldBe Some(
      "<Employee><EmployeeNumber>1</EmployeeNumber><FirstName>Joseph</FirstName><LastName>Smith</LastName><MiddleName>L</MiddleName><EmployeeStatus>Active</EmployeeStatus><EmployeeCategory>Line Pilot</EmployeeCategory><PilotCommentID>0</PilotCommentID><BaseCode>IAH</BaseCode><PreviousBaseCode>IAH</PreviousBaseCode><BaseSeniorityNumber>82</BaseSeniorityNumber><SystemSeniorityNumber>1a</SystemSeniorityNumber><StandardizedSeniorityNumber>1a</StandardizedSeniorityNumber><MasterSeniorityNumber>1a</MasterSeniorityNumber><ActiveSeniorityNumber>1a</ActiveSeniorityNumber><SeniorityDate>2022-05-29T12:00:29.0000000-05:00</SeniorityDate><EquipmentCode>737</EquipmentCode><PreviousEquipmentCode>737</PreviousEquipmentCode><PositionCode>CA</PositionCode><PreviousPositionCode>CA</PreviousPositionCode><EffectiveDate>2022-06-30T12:00:30.0000000-05:00</EffectiveDate><EffectiveEndDate>2022-07-29T11:59:29.0000000-05:00</EffectiveEndDate><ChangeType /><AssignmentType /><AssignmentTypeExternalName>Assignment based upon the SBA</AssignmentTypeExternalName><Absences /></Employee>",
    )
    reader.next() shouldBe Some(
      "<Employee><EmployeeNumber>1</EmployeeNumber><FirstName>Paul</FirstName><LastName>Wood</LastName><MiddleName>F</MiddleName><EmployeeStatus>Active</EmployeeStatus><EmployeeCategory>Line Pilot</EmployeeCategory><PilotCommentID>0</PilotCommentID><BaseCode>EWR</BaseCode><PreviousBaseCode>EWR</PreviousBaseCode><BaseSeniorityNumber>6</BaseSeniorityNumber><SystemSeniorityNumber>2b</SystemSeniorityNumber><StandardizedSeniorityNumber>2b</StandardizedSeniorityNumber><MasterSeniorityNumber>2b</MasterSeniorityNumber><ActiveSeniorityNumber>2b</ActiveSeniorityNumber><SeniorityDate>2021-03-10T12:00:10.0000000-05:00</SeniorityDate><EquipmentCode>756</EquipmentCode><PreviousEquipmentCode>756</PreviousEquipmentCode><PositionCode>CA</PositionCode><PreviousPositionCode>CA</PreviousPositionCode><EffectiveDate>2022-06-30T12:00:30.0000000-05:00</EffectiveDate><EffectiveEndDate>2022-07-29T11:59:29.0000000-05:00</EffectiveEndDate><ChangeType /><AssignmentType /><AssignmentTypeExternalName>Assignment based upon the SBA</AssignmentTypeExternalName><Absences><Absence><AbsenceType>ULA</AbsenceType><AbsenceStartDate>2022-11-20T12:00:20.0000000-06:00</AbsenceStartDate><AbsenceEndDate>2026-11-31T12:00:31.0000000-06:00</AbsenceEndDate></Absence><Absence><AbsenceType>VAC</AbsenceType><AbsenceStartDate>2022-06-25T12:00:26.0000000-05:00</AbsenceStartDate><AbsenceEndDate>2022-07-12T12:00:02.0000000-05:00</AbsenceEndDate></Absence></Absences></Employee>",
    )
    reader.next() shouldBe None
  }

  test("employee list xml tags one line") {
    val xml =
      """<?xml version="1.0" encoding="utf-8"?><A><Bid>202206</Bid><CreatedTimeStamp>2022-04-07T12:23:07.5034062-05:00</CreatedTimeStamp><CreatedBy>ME</CreatedBy><Carrier>FlyFly</Carrier><EmployeeList><Employee><EmployeeNumber>1</EmployeeNumber><FirstName>Joseph</FirstName><LastName>Smith</LastName><MiddleName>L</MiddleName><EmployeeStatus>Active</EmployeeStatus><EmployeeCategory>Line Pilot</EmployeeCategory><PilotCommentID>0</PilotCommentID><BaseCode>IAH</BaseCode><PreviousBaseCode>IAH</PreviousBaseCode><BaseSeniorityNumber>82</BaseSeniorityNumber><SystemSeniorityNumber>1a</SystemSeniorityNumber><StandardizedSeniorityNumber>1a</StandardizedSeniorityNumber><MasterSeniorityNumber>1a</MasterSeniorityNumber><ActiveSeniorityNumber>1a</ActiveSeniorityNumber><SeniorityDate>2022-05-29T12:00:29.0000000-05:00</SeniorityDate><EquipmentCode>737</EquipmentCode><PreviousEquipmentCode>737</PreviousEquipmentCode><PositionCode>CA</PositionCode><PreviousPositionCode>CA</PreviousPositionCode><EffectiveDate>2022-06-30T12:00:30.0000000-05:00</EffectiveDate><EffectiveEndDate>2022-07-29T11:59:29.0000000-05:00</EffectiveEndDate><ChangeType /><AssignmentType /><AssignmentTypeExternalName>Assignment based upon the SBA</AssignmentTypeExternalName><Absences /></Employee><Employee><EmployeeNumber>1</EmployeeNumber><FirstName>Paul</FirstName><LastName>Wood</LastName><MiddleName>F</MiddleName><EmployeeStatus>Active</EmployeeStatus><EmployeeCategory>Line Pilot</EmployeeCategory><PilotCommentID>0</PilotCommentID><BaseCode>EWR</BaseCode><PreviousBaseCode>EWR</PreviousBaseCode><BaseSeniorityNumber>6</BaseSeniorityNumber><SystemSeniorityNumber>2b</SystemSeniorityNumber><StandardizedSeniorityNumber>2b</StandardizedSeniorityNumber><MasterSeniorityNumber>2b</MasterSeniorityNumber><ActiveSeniorityNumber>2b</ActiveSeniorityNumber><SeniorityDate>2021-03-10T12:00:10.0000000-05:00</SeniorityDate><EquipmentCode>756</EquipmentCode><PreviousEquipmentCode>756</PreviousEquipmentCode><PositionCode>CA</PositionCode><PreviousPositionCode>CA</PreviousPositionCode><EffectiveDate>2022-06-30T12:00:30.0000000-05:00</EffectiveDate><EffectiveEndDate>2022-07-29T11:59:29.0000000-05:00</EffectiveEndDate><ChangeType /><AssignmentType /><AssignmentTypeExternalName>Assignment based upon the SBA</AssignmentTypeExternalName><Absences><Absence><AbsenceType>ULA</AbsenceType><AbsenceStartDate>2022-11-20T12:00:20.0000000-06:00</AbsenceStartDate><AbsenceEndDate>2026-11-31T12:00:31.0000000-06:00</AbsenceEndDate></Absence><Absence><AbsenceType>VAC</AbsenceType><AbsenceStartDate>2022-06-25T12:00:26.0000000-05:00</AbsenceStartDate><AbsenceEndDate>2022-07-12T12:00:02.0000000-05:00</AbsenceEndDate></Absence></Absences></Employee></EmployeeList></A>""".stripMargin

    val reader = new PrefixSuffixReader(createInputStream(xml), "<Employee>", "</Employee>")
    reader.next() shouldBe Some(
      "<Employee><EmployeeNumber>1</EmployeeNumber><FirstName>Joseph</FirstName><LastName>Smith</LastName><MiddleName>L</MiddleName><EmployeeStatus>Active</EmployeeStatus><EmployeeCategory>Line Pilot</EmployeeCategory><PilotCommentID>0</PilotCommentID><BaseCode>IAH</BaseCode><PreviousBaseCode>IAH</PreviousBaseCode><BaseSeniorityNumber>82</BaseSeniorityNumber><SystemSeniorityNumber>1a</SystemSeniorityNumber><StandardizedSeniorityNumber>1a</StandardizedSeniorityNumber><MasterSeniorityNumber>1a</MasterSeniorityNumber><ActiveSeniorityNumber>1a</ActiveSeniorityNumber><SeniorityDate>2022-05-29T12:00:29.0000000-05:00</SeniorityDate><EquipmentCode>737</EquipmentCode><PreviousEquipmentCode>737</PreviousEquipmentCode><PositionCode>CA</PositionCode><PreviousPositionCode>CA</PreviousPositionCode><EffectiveDate>2022-06-30T12:00:30.0000000-05:00</EffectiveDate><EffectiveEndDate>2022-07-29T11:59:29.0000000-05:00</EffectiveEndDate><ChangeType /><AssignmentType /><AssignmentTypeExternalName>Assignment based upon the SBA</AssignmentTypeExternalName><Absences /></Employee>",
    )
    reader.next() shouldBe Some(
      "<Employee><EmployeeNumber>1</EmployeeNumber><FirstName>Paul</FirstName><LastName>Wood</LastName><MiddleName>F</MiddleName><EmployeeStatus>Active</EmployeeStatus><EmployeeCategory>Line Pilot</EmployeeCategory><PilotCommentID>0</PilotCommentID><BaseCode>EWR</BaseCode><PreviousBaseCode>EWR</PreviousBaseCode><BaseSeniorityNumber>6</BaseSeniorityNumber><SystemSeniorityNumber>2b</SystemSeniorityNumber><StandardizedSeniorityNumber>2b</StandardizedSeniorityNumber><MasterSeniorityNumber>2b</MasterSeniorityNumber><ActiveSeniorityNumber>2b</ActiveSeniorityNumber><SeniorityDate>2021-03-10T12:00:10.0000000-05:00</SeniorityDate><EquipmentCode>756</EquipmentCode><PreviousEquipmentCode>756</PreviousEquipmentCode><PositionCode>CA</PositionCode><PreviousPositionCode>CA</PreviousPositionCode><EffectiveDate>2022-06-30T12:00:30.0000000-05:00</EffectiveDate><EffectiveEndDate>2022-07-29T11:59:29.0000000-05:00</EffectiveEndDate><ChangeType /><AssignmentType /><AssignmentTypeExternalName>Assignment based upon the SBA</AssignmentTypeExternalName><Absences><Absence><AbsenceType>ULA</AbsenceType><AbsenceStartDate>2022-11-20T12:00:20.0000000-06:00</AbsenceStartDate><AbsenceEndDate>2026-11-31T12:00:31.0000000-06:00</AbsenceEndDate></Absence><Absence><AbsenceType>VAC</AbsenceType><AbsenceStartDate>2022-06-25T12:00:26.0000000-05:00</AbsenceStartDate><AbsenceEndDate>2022-07-12T12:00:02.0000000-05:00</AbsenceEndDate></Absence></Absences></Employee>",
    )
    reader.next() shouldBe None
  }

  /*test("handle large file"){
    val resource = getClass.getResource("/PILOT_SENIORITY_22-07.xml")
    val reader = new PrefixSuffixReader(new FileInputStream(resource.getPath), "<Employee>", "</Employee>")
    //reader.next() shouldBe Some("<Employee><EmployeeNumber>U000685</EmployeeNumber><FirstName>Joseph</FirstName><LastName>Chan</LastName><MiddleName>L</MiddleName><EmployeeStatus>Active</EmployeeStatus><EmployeeCategory>Line Pilot</EmployeeCategory><PilotCommentID>0</PilotCommentID><BaseCode>IAH</BaseCode><PreviousBaseCode>IAH</PreviousBaseCode><BaseSeniorityNumber>82</BaseSeniorityNumber><SystemSeniorityNumber>1798</SystemSeniorityNumber><StandardizedSeniorityNumber>2490</StandardizedSeniorityNumber><MasterSeniorityNumber>5304</MasterSeniorityNumber><ActiveSeniorityNumber>3626</ActiveSeniorityNumber><SeniorityDate>1990-05-29T12:00:29.0000000-05:00</SeniorityDate><EquipmentCode>737</EquipmentCode><PreviousEquipmentCode>737</PreviousEquipmentCode><PositionCode>CA</PositionCode><PreviousPositionCode>CA</PreviousPositionCode><EffectiveDate>2022-06-30T12:00:30.0000000-05:00</EffectiveDate><EffectiveEndDate>2022-07-29T11:59:29.0000000-05:00</EffectiveEndDate><ChangeType /><AssignmentType /><AssignmentTypeExternalName>Assignment based upon the SBA</AssignmentTypeExternalName><Absences /></Employee>")
    var count = 0
    var next = reader.next()
    while(next.isDefined){
      count = count + 1
      next = reader.next()
    }
    println(count)
    reader.next() shouldBe None

    //xml to read the resource and count the number of tags <Employee>
    val xml = XML.load(resource.getPath)
    val employees = xml \\ "Employee"
    println(employees.size)
    employees.size shouldBe count
  }*/
  private def createInputStream(data: String): InputStream = new ByteArrayInputStream(data.getBytes)
}

//Some("<Employee><EmployeeNumber>U000685</EmployeeNumber><FirstName>Joseph</FirstName><LastName>Chan</LastName><MiddleName>L</MiddleName><EmployeeStatus>Active</EmployeeStatus><EmployeeCategory>Line Pilot</EmployeeCategory><PilotCommentID>0</PilotCommentID><BaseCode>IAH</BaseCode><PreviousBaseCode>IAH</PreviousBaseCode><BaseSeniorityNumber>82</BaseSeniorityNumber><SystemSeniorityNumber>1798</SystemSeniorityNumber><StandardizedSeniorityNumber>2490</StandardizedSeniorityNumber><MasterSeniorityNumber>5304</MasterSeniorityNumber><ActiveSeniorityNumber>3626</ActiveSeniorityNumber><SeniorityDate>1990-05-29T12:00:29.0000000-05:00</SeniorityDate><EquipmentCode>737</EquipmentCode><PreviousEquipmentCode>737</PreviousEquipmentCode><PositionCode>CA</PositionCode><PreviousPositionCode>CA</PreviousPositionCod<Employee><EmployeeNumber>U000685</EmployeeNumber><FirstName>Joseph</FirstName><LastName>Chan</LastName><MiddleName>L</MiddleName><EmployeeStatus>Active</EmployeeStatus><EmployeeCategory>Line Pilot</EmployeeCategory><PilotCommentID>0</PilotCommentID><BaseCode>IAH</BaseCode><PreviousBaseCode>IAH</PreviousBaseCode><BaseSeniorityNumber>82</BaseSeniorityNumber><SystemSeniorityNumber>1798</SystemSeniorityNumber><StandardizedSeniorityNumber>2490</StandardizedSeniorityNumber><MasterSeniorityNumber>5304</MasterSeniorityNumber><ActiveSeniorityNumber>3626</ActiveSeniorityNumber><SeniorityDate>1990-05-29T12:00:29.0000000-05:00</SeniorityDate><EquipmentCode>737</EquipmentCode><PreviousEquipmentCode>737</PreviousEquipmentCode><PositionCode>CA</PositionCode><PreviousPositionCode>CA</PreviousPositionCode><EffectiveDate>2022-06-30T12:00:30.0000000-05:00</EffectiveDate><EffectiveEndDate>2022-07-29T11:59:29.0000000-05:00</EffectiveEndDate><ChangeType /><AssignmentType /><AssignmentTypeExternalName>Assignment based upon the SBA</AssignmentTypeExternalName><Absences /></Employee>")
//Some("<Employee><EmployeeNumber>U000685</EmployeeNumber><FirstName>Joseph</FirstName><LastName>Chan</LastName><MiddleName>L</MiddleName><EmployeeStatus>Active</EmployeeStatus><EmployeeCategory>Line Pilot</EmployeeCategory><PilotCommentID>0</PilotCommentID><BaseCode>IAH</BaseCode><PreviousBaseCode>IAH</PreviousBaseCode><BaseSeniorityNumber>82</BaseSeniorityNumber><SystemSeniorityNumber>1798</SystemSeniorityNumber><StandardizedSeniorityNumber>2490</StandardizedSeniorityNumber><MasterSeniorityNumber>5304</MasterSeniorityNumber><ActiveSeniorityNumber>3626</ActiveSeniorityNumber><SeniorityDate>1990-05-29T12:00:29.0000000-05:00</SeniorityDate><EquipmentCode>737</EquipmentCode><PreviousEquipmentCode>737</PreviousEquipmentCode><PositionCode>CA</PositionCode><PreviousPositionCode>CA</PreviousPositionCode><EffectiveDate>2022-06-30T12:00:30.0000000-05:00</EffectiveDate><EffectiveEndDate>2022-07-29T11:59:29.0000000-05:00</EffectiveEndDate><ChangeType /><AssignmentType /><AssignmentTypeExternalName>Assignment based upon the SBA</AssignmentTypeExternalName><Absences /></Employee>")
