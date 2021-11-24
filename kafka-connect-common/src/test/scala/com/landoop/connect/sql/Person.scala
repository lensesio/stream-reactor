package com.landoop.connect.sql

case class Person(name: String, address: Address)

case class Address(street: Street, street2: Option[Street], city: String, state: String, zip: String, country: String)

case class Street(name: String)

case class SimpleAddress(street: String, city: String, state: String, zip: String, country: String)
