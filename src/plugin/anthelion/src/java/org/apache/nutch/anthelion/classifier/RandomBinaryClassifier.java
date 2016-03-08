/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nutch.anthelion.classifier;

import java.util.Random;

import moa.classifiers.AbstractClassifier;
import moa.core.Measurement;
import moa.core.StringUtils;
import weka.core.Instance;

/**
 * Classifier which pseudo randomly assigns true/false (0/1) to unlabeled
 * instances.
 * 
 * @author Robert Meusel (robert@dwslab.de)
 * 
 */
public class RandomBinaryClassifier extends AbstractClassifier {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	Random rnd = new Random();

	@Override
	public boolean isRandomizable() {
		return false;
	}

	@Override
	public double[] getVotesForInstance(Instance inst) {
		double[] re = new double[2];
		if (rnd.nextBoolean()) {
			re[0] = 0;
			re[1] = 1;
		} else {
			re[0] = 1;
			re[1] = 0;
		}
		return re;
	}

	@Override
	public void resetLearningImpl() {
		rnd = new Random();
	}

	@Override
	public void trainOnInstanceImpl(Instance inst) {
		// Rnd is Rnd
	}

	@Override
	protected Measurement[] getModelMeasurementsImpl() {
		return null;
	}

	@Override
	public void getModelDescription(StringBuilder out, int indent) {
		StringUtils.appendNewlineIndented(out, indent,
				"BinaryRandomClassifiere is random and returns [1,0] or [0,1]");
		StringUtils.appendNewline(out);
	}
}
