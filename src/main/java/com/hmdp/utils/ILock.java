package com.hmdp.utils;

/**
 * ClassName: ILock
 * Package: com.hmdp.utils
 * Description:
 *
 * @Author: Jerry
 * @Date: 2023/8/22
 */
public interface ILock {

    boolean tryLock(long timeoutSec);

    void unlock();
}
