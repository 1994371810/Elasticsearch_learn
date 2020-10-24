package com.mt.bean;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.DateFormat;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.annotations.Field;
import org.springframework.data.elasticsearch.annotations.FieldType;
import org.springframework.format.annotation.DateTimeFormat;

import java.util.Date;
import java.util.List;

/**
 * Created by 郭俊旺 on 2020/10/22 15:24
 *
 * @author 郭俊旺
 */
@Document(indexName = "student")
@NoArgsConstructor
@AllArgsConstructor
@Data
public class Student {

    @Id
    public Integer userId;

    @Field(type = FieldType.Keyword)
    public String name;

    @Field(type = FieldType.Integer)
    public Integer age;

    @Field(type = FieldType.Date,format = DateFormat.date_optional_time)
    public Date birthday;


    @Field(type = FieldType.Auto)
    public List<String> tag;


    @Field(type = FieldType.Text,analyzer = "ik_max_word")
    public String hobby;

}
