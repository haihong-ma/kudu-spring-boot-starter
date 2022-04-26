package com.kad.cube.kudu.config;

import com.kad.cube.kudu.core.KuduOperations;
import com.kad.cube.kudu.connection.KuduSessionFactory;
import com.kad.cube.kudu.core.KuduTemplate;
import com.kad.cube.kudu.core.converter.DefaultKuduConverter;
import com.kad.cube.kudu.core.converter.KuduConverter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

/**
 * @author haihong.ma
 */
@Configuration
@ConditionalOnClass({KuduOperations.class})
@Import({KuduSessionConfiguration.class})
public class KuduAutoConfiguration {
    public KuduAutoConfiguration() {
    }

    @Bean
    @ConditionalOnMissingBean(name = {"kuduTemplate"})
    public KuduTemplate kuduTemplate(KuduConverter kuduConverter, KuduSessionFactory kuduSessionFactory) {
        KuduTemplate kuduTemplate = new KuduTemplate(kuduConverter);
        kuduTemplate.setKuduSessionFactory(kuduSessionFactory);
        return kuduTemplate;
    }

    @Bean
    @ConditionalOnMissingBean({KuduConverter.class})
    public KuduConverter kuduConverter() {
        return new DefaultKuduConverter();
    }
}