package com.landoop.json.sql

case class Ingredient(name: String, sugar: Double, fat: Double)

case class Pizza(name: String, ingredients: Seq[Ingredient], vegetarian: Boolean, vegan: Boolean, calories: Int)

