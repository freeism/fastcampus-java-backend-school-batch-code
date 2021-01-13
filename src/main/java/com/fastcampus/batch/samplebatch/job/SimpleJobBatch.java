package com.fastcampus.batch.samplebatch.job;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author Martin
 * @since 2021/01/13
 * @link https://docs.spring.io/spring-batch/docs/current/reference/html/index.html
 */
@Configuration
@RequiredArgsConstructor
public class SimpleJobBatch {
    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;

    @Bean
    public Job simpleJob() {
        return jobBuilderFactory.get("simpleJob")
            .start(simpleStep1(null))
            .next(simpleStep2())
            .build();
    }

    @Bean
    public Job simpleJob2() {
        return jobBuilderFactory.get("simpleJob2")
            .start(simpleStep1(null))
//            .next(simpleStep2())
            .build();
    }

    // spring lifecycle : type=request

    @Bean
    @JobScope      // Run/debug Configuration에서 Program Arguments 에 --job.name=simpleJob requestDate=2020-01-13 추가해줌
    public Step simpleStep1(@Value("#{jobParameters[requestDate]}") String requestDate) {
        return stepBuilderFactory.get("simpleStep1")
            .tasklet(((contribution, chunkContext) -> {
                List<User> users = new ArrayList<>(); //repo.findByEmailLimit100();
                System.out.println(">>> requestDate : " + requestDate);
                System.out.println(">>> simpleStep1 실행");

                if (users.size() == 0) {
                    return RepeatStatus.FINISHED;
                }

                // processing logic 추가
                return RepeatStatus.CONTINUABLE;
            }))
            .build();
    }

    @Bean
    public Step simpleStep2() {
        return stepBuilderFactory.get("simpleStep2")
            .<User, Member>chunk(10)
            .reader(itemReader())
            .processor(itemProcessor())
            .writer(new CustomWriter())
            .build();
    }

    @Bean
    public ItemReader<User> itemReader() {
        return new QueueItemReader();
    }

    @Bean
    public ItemProcessor<User, Member> itemProcessor() {
        return new CustomProcessor();
    }

    private static class QueueItemReader implements ItemReader<User> {
        private LinkedList<User> queue = User.dummyList();

        @Override
        public User read() throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
            return queue.poll();
        }
    }

    private static class CustomWriter implements ItemWriter<Member> {
        @Override
        public void write(List<? extends Member> items) throws Exception {
            System.out.println(">>>" + items + " write");
        }
    }

    private static class CustomProcessor implements ItemProcessor<User, Member> {
        @Override
        public Member process(User item) throws Exception {
            System.out.println(">>>" + item + " processing");
            return new Member(item.getName());
        }
    }

    @Data
    @AllArgsConstructor
    private static class User {
        private String name;

        public static LinkedList<User> dummyList() {
            LinkedList<User> users = new LinkedList<>();
            users.add(new User("martin"));
            users.add(new User("dennis"));

            return users;
        }
    }

    @Data
    @AllArgsConstructor
    private static class Member {
        private String name;
    }
}
