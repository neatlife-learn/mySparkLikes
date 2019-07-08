package mysparkkafkalikes.controller;

import lombok.RequiredArgsConstructor;
import mysparkkafkalikes.service.Producer;
import mysparkkafkalikes.service.SparkLikeService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author suxiaolin
 * @date 2019-06-28 12:03
 */
@RestController
@RequestMapping("/")
@RequiredArgsConstructor(onConstructor = @__(@Autowired))
public class MyController {

    private final Producer producer;

    private final SparkLikeService sparkLikeService;

    @RequestMapping("/send-like")
    public String sendLike(@RequestParam(value = "post_id", required = true) Integer postId) {
        producer.send(postId);
        return "test1";
    }

    @RequestMapping("/launch")
    public String launch() {
        sparkLikeService.launch();
        return "test2";
    }
}
