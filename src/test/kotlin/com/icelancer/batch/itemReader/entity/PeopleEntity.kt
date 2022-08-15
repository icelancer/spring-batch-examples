package com.icelancer.batch.itemReader.entity

import javax.persistence.Column
import javax.persistence.Entity
import javax.persistence.Id
import javax.persistence.Table

@Entity
@Table(name = "people")
data class PeopleEntity(
    @Id
    @Column(name = "people_id")
    val peopleId: Int,
    val firstName: String,
    val lastName: String,
    val age: String,
    val gender: String,
    val pick: String
)
