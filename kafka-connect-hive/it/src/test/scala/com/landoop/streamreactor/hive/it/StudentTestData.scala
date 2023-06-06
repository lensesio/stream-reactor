/*
 * Copyright 2020 Lenses.io
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

package com.landoop.streamreactor.hive.it

import scala.util.Random

class DepartmentAssigner(val numGroups: Int) {
  var currentCount: Int = 0

  def increaseOrReset = {
    val ret = currentCount
    if (currentCount >= numGroups) {
      currentCount = 0
    } else {
      currentCount += 1
    }
    ret
  }
}

class StudentTestData {

  val names = Seq("Deana", "Annalisa", "Rosa", "Paulene", "Tiny", "Olga", "Daphne", "Libby", "Krystal", "Britni", "Miguel",
    "Don", "Cherry", "Catharine", "Phyllis", "Ursula", "Nicky", "Romeo", "Carolyn", "Melissa", "Lorna", "Sandy", "Cindy",
    "Toby", "Toni", "Karen", "Maryam", "Ada", "Barney", "Ferdinand", "Melissa", "Barbara", "Alycia", "Anita", "Kelly", "Harrison")

  val deptAssigner = new DepartmentAssigner(5)

  case class Student(department_number: Int, student_name: String)

  def createStudent: Student = {
    val stud = Student(
      deptAssigner.increaseOrReset,
      names(Random.nextInt(names.length))
    )
    stud
  }

}
