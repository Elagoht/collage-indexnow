// A collage plugin that tells search engines which URLs changed, through the
// IndexNow protocol Bing, Yandex, Seznam and others read: the paths a cache
// invalidation dropped, batched, sent in the background, retried when the
// endpoint is busy.
//
// It requires collage the way any consumer does, and reaches nothing the framework
// does not offer every plugin.
module github.com/Elagoht/collage-indexnow

go 1.26

require github.com/Elagoht/collage v0.42.0
