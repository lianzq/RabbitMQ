package cn.lzq.mq.service;

import com.alibaba.fastjson.JSONObject;
import org.springframework.stereotype.Service;

/**
 * Created by lianzhiqiang on 2017/7/21.
 */
@Service("jsonMessageProcessImpl")
public class JsonMessageProcessImpl implements MessageProcess {

    @Override
    public boolean process(String message) {
        JSONObject jsonObject = JSONObject.parseObject(message);
        System.out.println(jsonObject);
        // 业务逻辑
        return true;
    }
}
