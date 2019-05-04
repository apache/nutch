// test data for link extraction from "pure" JavaScript

function selectProvider(form) {
    provider = form.elements['searchProvider'].value;
    if (provider == "any") {
        if (Math.random() > 0.5) {
            provider = "lucid";
        } else {
            provider = "sl";
        }
    }

    if (provider == "lucid") {
        form.action = "http://search.lucidimagination.com/p:nutch";
    } else if (provider == "sl") {
        form.action = "http://search-lucene.com/nutch";
    }

    days = 90; // cookie will be valid for 90 days
    date = new Date();
    date.setTime(date.getTime() + (days * 24 * 60 * 60 * 1000));
    expires = "; expires=" + date.toGMTString();
    document.cookie = "searchProvider=" + provider + expires + "; path=/";
}
