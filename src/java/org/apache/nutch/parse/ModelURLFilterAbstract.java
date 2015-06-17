package org.apache.nutch.parse;

import org.apache.nutch.net.URLFilter;

public abstract class ModelURLFilterAbstract implements URLFilter{

	
	public abstract void filterParse(String text);
	public abstract boolean filterUrl(String url) ;
	public abstract void configure(String[] args) ;
	
}
