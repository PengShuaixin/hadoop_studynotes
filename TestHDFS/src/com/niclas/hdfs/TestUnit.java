package com.niclas.hdfs;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
//�������
public class TestUnit {
	@Before
	public void before() {
		System.out.println("��ʼ��ı���");
	}
	@Test
	public void test() {
		System.out.println("����");
	}

	@Test
	public void test2() {
		System.out.println("����");
	}
	@After
	public void after() {
		System.out.println("����");
	}
}
