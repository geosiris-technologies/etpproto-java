/*
Copyright 2019 GEOSIRIS

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package com.geosiris.etp.utils;

public class Pair<T, U> {

	private T left;
	private U right;
	
	public Pair(T left, U right) {
		this.left = left;
		this.right = right;
	}
	
	public T l() {
		return left;
	}
	
	public U r() {
		return right;
	}

	public void setLeft(T left) {
		this.left = left;
	}

	public void setRight(U right) {
		this.right = right;
	}

	@Override
	public String toString() {
		return "( " + left + " ; " + right +" )";
	}

}
