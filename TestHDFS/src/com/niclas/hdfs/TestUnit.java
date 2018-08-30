package com.niclas.hdfs;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
//单点测试
public class TestUnit {
	@Before
	public void before() {
		System.out.println("开始你的表演");
	}
	@Test
	public void test() {
		System.out.println("唱歌");
	}

	@Test
	public void test2() {
		System.out.println("跳舞");
	}
	@After
	public void after() {
		System.out.println("结束");
	}
}
