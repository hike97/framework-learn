package com.atguigu.redis.service;

import com.atguigu.redis.entities.User;
import com.atguigu.redis.mapper.UserMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * @auther zzyy
 * @create 2021-05-01 14:58
 * @desc redis 和 mysql 增删改查
 */
@Service
@Slf4j
public class UserService {

    public static final String CACHE_KEY_USER = "user:";

    @Resource
    private UserMapper userMapper;
    @Resource
    private RedisTemplate redisTemplate;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    // 定义一个Random对象作为类变量
    private Random random = new Random();

    // 定义一个常量表示日期的前缀
    private static final String DATE_PREFIX = "202306";

    /**
     * mysql 成功后 才能操作redis
     *
     * @param user
     */
    public void addUser(User user) {
        //1 先插入mysql并成功
        int i = userMapper.insertSelective(user);

        if (i > 0) {
            //2 需要再次查询一下mysql将数据捞回来并ok
            user = userMapper.selectByIdAndOffset(user.getId());
            //3 将捞出来的user存进redis，完成新增功能的数据一致性。
            String key = CACHE_KEY_USER + user.getId();
            redisTemplate.opsForValue().set(key, user);
        }
    }

    public void deleteUser(User id) {
        int i = userMapper.deleteByPrimaryKey(id);
        if (i > 0) {
            String key = CACHE_KEY_USER + id;
            redisTemplate.delete(key);
        }
    }

    public void updateUser(User user) {
        int i = userMapper.updateByPrimaryKeySelective(user);
        if (i > 0) {
            //2 需要再次查询一下mysql将数据捞回来并ok
            user = userMapper.selectByPrimaryKey(user);
            //3 将捞出来的user存进redis，完成修改
            String key = CACHE_KEY_USER + user.getId();
            redisTemplate.opsForValue().set(key, user);
        }
    }

    /**
     * 业务逻辑并没有写错，对于小厂中厂(QPS《=1000)可以使用，但是大厂不行
     *
     * @param id
     * @return
     */
    public User findUserById(Integer id) {
        User user = null;
        String key = CACHE_KEY_USER + id;

        //1 先从redis里面查询，如果有直接返回结果，如果没有再去查询mysql
        user = (User) redisTemplate.opsForValue().get(key);

        if (user == null) {
            //2 redis里面无，继续查询mysql
            user = userMapper.selectByPrimaryKey(id);
            if (user == null) {
                //3.1 redis+mysql 都无数据
                //你具体细化，防止多次穿透，我们规定，记录下导致穿透的这个key回写redis
                return user;
            } else {
                //3.2 mysql有，需要将数据写回redis，保证下一次的缓存命中率
                redisTemplate.opsForValue().set(key, user);
            }
        }
        return user;
    }


    /**
     * 加强补充，避免突然key失效了，打爆mysql，做一下预防，尽量不出现击穿的情况。
     *
     * @param id
     * @return
     */
    public User findUserById2(Integer id) {
        User user;
        String key = CACHE_KEY_USER + id;

        //1 先从redis里面查询，如果有直接返回结果，如果没有再去查询mysql
        user = (User) redisTemplate.opsForValue().get(key);

        if (user == null) {
            //2 大厂用，对于高QPS的优化，进来就先加锁，保证一个请求操作，让外面的redis等待一下，避免击穿mysql
            synchronized (UserService.class) {
                user = (User) redisTemplate.opsForValue().get(key);
                //3 二次查redis还是null，可以去查mysql了(mysql默认有数据)
                if (user == null) {
                    //4 查询mysql拿数据
                    user = userMapper.selectByPrimaryKey(id);//mysql有数据默认
                    if (user == null) {
                        // 不存在则返回空值并设置缓存时间为60秒，防止缓存穿透
                        stringRedisTemplate.opsForValue().set(key, "", 60, TimeUnit.SECONDS);
                        return null;
                    } else {
                        //5 mysql里面有数据的，需要回写redis，完成数据一致性的同步工作
                        redisTemplate.opsForValue().setIfAbsent(key, user, 7L, TimeUnit.DAYS);
                    }
                }
            }
        }
        return user;
    }

    /**
     * 随机登录方法
     */
    public void simulateLogin(){
        //获取全部user
        List<User> users = userMapper.selectAll();
        for (User user : users) {
            randomLogin(user.getId(),user.getOffset());
        }
    }

    // 定义一个随机登录的方法，参数是用户的id和offset
    public void randomLogin(int userId, int offset) {
        for (int i = 1; i <= 30; i++) {
            // 格式化日期为202306xx的形式，其中xx是两位数的日期
            String date = String.format(DATE_PREFIX + "%02d", i);
            // 随机生成一个布尔值，表示用户是否登录
            boolean login = random.nextBoolean();
            // 如果用户登录了，就使用RedisTemplate的setbit方法，将用户对应的日期和偏移量设置为1
            if (login) {
                redisTemplate.opsForValue().setBit(date, offset, true);
                // 打印一条日志信息
                System.out.println("User " + userId + " logged in on " + date);
            }
        }
    }

    /**
     * 统计用户活跃度的方法
     */
    /**
     * 计算本月有多少用户登录的方法
     */
    public void countLoginUsers() {
        // 定义一个BitSet对象来存储本月所有用户的登录情况
        BitSet login = new BitSet(10000);
        // 遍历6月份的每一天
        for (int i = 1; i <= 30; i++) {
            // 格式化日期为202306xx的形式，其中xx是两位数的日期
            String date = String.format(DATE_PREFIX + "%02d", i);
            // 使用RedisTemplate的get方法，获取当前日期对应的key的值，转换为字节数组
            byte[] bytes = (byte[]) redisTemplate.opsForValue().get(date);
            // 如果字节数组不为空，就将其转换为BitSet对象，并与login进行或运算
            if (bytes != null) {
                BitSet bitSet = BitSet.valueOf(bytes);
                login.or(bitSet);
            }
        }
        // 使用BitSet的cardinality方法，统计login中有多少位为1，即有多少用户登录过
        int loginUsers = login.cardinality();
        // 打印一条日志信息
        System.out.println("There are " + loginUsers + " users who logged in June.");
    }

    /**
     * 计算连续登录两天的用户有多少，分别是谁的方法
     */
    public void countContinuousLoginUsers() {
        // 定义一个变量来记录连续登录两天的用户的数量
        int continuousLoginUsers = 0;
        // 定义一个列表来存储连续登录两天的用户的id
        List<Integer> userIds = new ArrayList<>();
        // 定义一个BitSet对象来存储前一天所有用户的登录情况
        BitSet prevLogin = new BitSet(10000);
        // 遍历6月份的每一天，从第一天开始
        for (int i = 1; i <= 30; i++) {
            // 格式化日期为202306xx的形式，其中xx是两位数的日期
            String date = String.format(DATE_PREFIX + "%02d", i);
            // 使用RedisTemplate的get方法，获取当前日期对应的key的值，转换为字节数组
            byte[] bytes = (byte[]) redisTemplate.opsForValue().get(date);
            // 如果字节数组不为空，就将其转换为BitSet对象，并与prevLogin进行与运算，结果存储到一个新的BitSet对象中，命名为continuousLogin
            BitSet bitSet = null;
            if (bytes != null) {
                bitSet = BitSet.valueOf(bytes);
                BitSet continuousLogin = (BitSet) prevLogin.clone();
                continuousLogin.and(bitSet);
                // 使用BitSet的cardinality方法，统计continuousLogin中有多少位为1，即有多少用户连续登录两天
                int continuousLoginCount = continuousLogin.cardinality();
                // 如果有用户连续登录两天，就将连续登录两天的用户的数量加上这个值，并将用户id添加到列表中
                if (continuousLoginCount > 0) {
                    continuousLoginUsers += continuousLoginCount;
                    // 遍历continuousLogin中的每一位
                    for (int j = continuousLogin.nextSetBit(0); j >= 0; j = continuousLogin.nextSetBit(j + 1)) {
                        // 将对应的用户id添加到列表中
                        userIds.add(j + 1);
                    }
                }
            }
            // 将当前日期对应的BitSet对象赋值给prevLogin，作为下一次循环的前一天
            prevLogin = bitSet;
        }
        // 打印一条日志信息
        System.out.println("There are " + continuousLoginUsers + " users who logged in continuously for two days in June. They are: " + userIds);
    }



}

