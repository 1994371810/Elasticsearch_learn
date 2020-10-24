package com.mt.dao;

import com.mt.bean.Student;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;
import org.springframework.data.elasticsearch.repository.ReactiveElasticsearchRepository;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;

import java.util.Date;
import java.util.List;

/**
 * Created by 郭俊旺 on 2020/10/22 16:53
 *
 * @author 郭俊旺
 */
public interface StudentDao extends ElasticsearchRepository<Student, Integer> {

    List<Student> queryByNameLikeAndHobbyLike(String name,String hobby);

    List<Student> queryByBirthdayBetween(Date left,Date right);


}
