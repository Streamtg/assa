const UPSTREAM_ORIGIN = '__PINGGY_URL__'

addEventListener('fetch', event => {
  event.respondWith(handleRequest(event.request))
})

async function handleRequest(request) {
  const url = new URL(request.url)
  const { pathname, searchParams } = url
  const host = request.headers.get('host') || 'localhost'
  const baseUrl = `https://${host}`

  if (request.method === 'OPTIONS') {
    return new Response(null, { status: 204, headers: corsHeaders() })
  }

  if (pathname === '/' || pathname === '/index.html') {
    return htmlResponse(homePageHtml(), { 'Cache-Control': 'public, max-age=600' })
  }

  if (pathname === '/robots.txt') {
    return new Response(`User-agent: *\nAllow: /\nSitemap: ${baseUrl}/sitemap.xml`, {
      headers: { 'Content-Type': 'text/plain; charset=utf-8' }
    })
  }

  if (pathname === '/sitemap.xml') {
    const t = new Date().toISOString().split('T')[0]
    return new Response(`<?xml version="1.0" encoding="UTF-8"?><urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9"><url><loc>${baseUrl}/</loc><lastmod>${t}</lastmod></url></urlset>`, {
      headers: { 'Content-Type': 'application/xml; charset=utf-8' }
    })
  }

  const pathParts = pathname.split('/').filter(Boolean)
if (pathParts.length < 2) {
    return htmlResponse(homePageHtml(), { 'Cache-Control': 'public, max-age=600' })
}

// Nuevo formato del Bot: /{hash}/{filename}
const hash = pathParts[0]
const filename = pathParts[1] || ''
const messageID = hash
if (!hash) return new Response('Invalid link', { status: 400 })

  const action = searchParams.get('action') || ''
  const player = normalizePlayer(searchParams.get('player') || 'plyr')
  const qCh = searchParams.get('ch') || ''
  const qFn = searchParams.get('fn') || ''
  const qMt = normalizeMime(searchParams.get('mt') || '')
  const qFs = searchParams.get('fs') ? parseInt(searchParams.get('fs'), 10) : 0

  let upstreamUrl = `${UPSTREAM_ORIGIN}${pathname}`
  if (qCh) upstreamUrl += `?ch=${encodeURIComponent(qCh)}`

  function buildActionUrl(act, pl) {
    const p = new URLSearchParams()
    if (qCh) p.set('ch', qCh)
    if (qFn) p.set('fn', qFn)
    if (qMt) p.set('mt', qMt)
    if (qFs) p.set('fs', String(qFs))
    p.set('action', act)
    if (pl) p.set('player', pl)
    return `${baseUrl}${pathname}?${p.toString()}`
  }

  const streamUrl = buildActionUrl('stream', '')
  const downloadUrl = buildActionUrl('download', '')

  if (action === 'stream') {
    return proxyFile({ request, upstreamUrl, dispositionMode: 'inline', fallbackFilename: qFn, fallbackMime: qMt, messageID, hash })
  }

  if (action === 'download') {
    return proxyFile({ request, upstreamUrl, dispositionMode: 'attachment', fallbackFilename: qFn, fallbackMime: qMt, messageID, hash })
  }

  let meta
  if (qFn && qMt) {
    meta = { filename: qFn, mime: qMt, contentLength: qFs || null }
  } else {
    meta = await fetchMetadata(upstreamUrl, messageID, hash)
    if (qFn) meta.filename = qFn
    if (qMt) meta.mime = qMt
    if (qFs) meta.contentLength = qFs
  }

  meta.mime = normalizeMime(meta.mime || 'application/octet-stream')
  if (!hasExtension(meta.filename)) meta.filename += getExtensionFromMime(meta.mime)

  const canon = new URLSearchParams()
  if (qCh) canon.set('ch', qCh)
  if (qFn) canon.set('fn', qFn)
  if (qMt) canon.set('mt', qMt)
  if (qFs) canon.set('fs', String(qFs))
  if (player !== 'plyr') canon.set('player', player)
  const pageUrl = `${baseUrl}${pathname}${canon.toString() ? '?' + canon.toString() : ''}`

  return htmlResponse(buildPlayerHtml({
    safeTitle: safeText(meta.filename, 180),
    streamUrl,
    downloadUrl,
    realMime: meta.mime,
    realFilename: meta.filename,
    contentLength: meta.contentLength,
    pathname,
    player,
    qCh,
    qFn,
    qMt,
    qFs,
    pageUrl
  }), { 'Cache-Control': 'public, max-age=600' })
}

async function proxyFile({ request, upstreamUrl, dispositionMode, fallbackFilename, fallbackMime, messageID, hash }) {
  try {
    const range = request.headers.get('Range') || request.headers.get('range') || ''
    const extraHeaders = range ? { Range: range } : {}
    const result = await smartFetch(upstreamUrl, extraHeaders)

    if (result.status !== 200 && result.status !== 206) {
      return new Response('Upstream error ' + result.status, { status: result.status >= 400 ? 502 : result.status })
    }

    let mime = normalizeMime(fallbackMime || result.mime || 'application/octet-stream')
    let filename = fallbackFilename || result.filename || generateFilename(messageID, hash, mime)
    if (!hasExtension(filename)) filename += getExtensionFromMime(mime)
    filename = sanitizeFilenameForHeader(filename)

    const out = new Headers()
    out.set('Content-Type', mime)
    out.set('Accept-Ranges', result.headers.get('accept-ranges') || 'bytes')
    out.set('Cache-Control', dispositionMode === 'attachment' ? 'no-store' : 'public, max-age=3600')
    out.set('Access-Control-Allow-Origin', '*')
    out.set('Access-Control-Allow-Methods', 'GET,HEAD,OPTIONS')
    out.set('Access-Control-Allow-Headers', 'Range,Content-Type')
    out.set('Access-Control-Expose-Headers', 'Content-Length,Content-Range,Content-Type,Accept-Ranges,Content-Disposition')
    out.set('Content-Disposition', `${dispositionMode}; filename="${filename}"; filename*=UTF-8''${encodeURIComponent(filename)}`)

    const cr = result.headers.get('content-range')
    const cl = result.headers.get('content-length')
    if (cr) out.set('Content-Range', cr)
    if (cl) out.set('Content-Length', cl)

    return new Response(result.body, { status: result.status, headers: out })
  } catch (e) {
    return new Response('Proxy error: ' + e.message, { status: 502 })
  }
}

async function smartFetch(u, h = {}) {
  const r = await fetch(u, {
    method: 'GET',
    headers: { Accept: '*/*', 'Accept-Encoding': 'identity', 'User-Agent': 'Mozilla/5.0', ...h }
  })

  const headerFilename = extractFilename(r.headers.get('content-disposition') || '')
  const urlFilename = extractFilenameFromUrl(u)
  const filename = headerFilename || urlFilename
  let finalMime = normalizeMime(parseMime(r.headers.get('content-type')))

  if (isWeakMime(finalMime)) {
    const extMime = getMimeFromFilename(filename || u)
    if (extMime) finalMime = extMime
  }

  let finalBody = r.body
  if (isWeakMime(finalMime) && r.body) {
    try {
      const rd = r.body.getReader()
      const { value: firstChunk } = await rd.read()
      if (firstChunk && firstChunk.length > 0) {
        const detected = detectMimeFromBytes(firstChunk)
        if (detected) finalMime = detected
      }
      let pending = firstChunk || null
      finalBody = new ReadableStream({
        async pull(controller) {
          try {
            if (pending !== null) {
              controller.enqueue(pending)
              pending = null
              return
            }
            const { done, value } = await rd.read()
            if (done) return controller.close()
            controller.enqueue(value)
          } catch (e) {
            controller.error(e)
          }
        },
        cancel() { return rd.cancel() }
      })
    } catch (_) {}
  }

  if (!finalMime) finalMime = 'application/octet-stream'
  return { status: r.status, headers: r.headers, mime: normalizeMime(finalMime), body: finalBody, contentLength: parseSize(r.headers), filename }
}

async function fetchMetadata(pu, mid, h) {
  let filename = ''
  let mime = 'application/octet-stream'
  let contentLength = null
  const urlFilename = extractFilenameFromUrl(pu)

  try {
    const r = await fetch(pu, { method: 'HEAD', headers: { Accept: '*/*', 'Accept-Encoding': 'identity', 'User-Agent': 'Mozilla/5.0' } })
    if (r.ok || r.status === 206) {
      filename = extractFilename(r.headers.get('content-disposition') || '') || urlFilename
      const contentType = normalizeMime(parseMime(r.headers.get('content-type')))
      if (!isWeakMime(contentType)) mime = contentType
      contentLength = parseSize(r.headers)
      if (isWeakMime(mime) && filename) {
        const extMime = getMimeFromFilename(filename)
        if (extMime) mime = extMime
      }
    }
  } catch (_) {}

  if (isWeakMime(mime) || !filename || !contentLength) {
    try {
      const r = await fetch(pu, { method: 'GET', headers: { Range: 'bytes=0-8191', Accept: '*/*', 'Accept-Encoding': 'identity', 'User-Agent': 'Mozilla/5.0' } })
      if (r.ok || r.status === 206) {
        if (!filename) filename = extractFilename(r.headers.get('content-disposition') || '') || urlFilename
        if (!contentLength) contentLength = parseSize(r.headers)
        const contentType = normalizeMime(parseMime(r.headers.get('content-type')))
        if (!isWeakMime(contentType)) mime = contentType
        if (isWeakMime(mime) && filename) {
          const extMime = getMimeFromFilename(filename)
          if (extMime) mime = extMime
        }
        if (isWeakMime(mime) && r.body) {
          const rd = r.body.getReader()
          const { value } = await rd.read()
          await rd.cancel()
          if (value && value.length > 0) {
            const detected = detectMimeFromBytes(value)
            if (detected) mime = detected
          }
        } else {
          try { if (r.body) await r.body.cancel() } catch (_) {}
        }
      }
    } catch (_) {}
  }

  mime = normalizeMime(mime || 'application/octet-stream')
  if (!filename) filename = generateFilename(mid, h, mime)
  else if (!hasExtension(filename)) filename += getExtensionFromMime(mime)
  return { filename, mime, contentLength }
}

function detectMimeFromBytes(bytes) {
  if (!bytes || bytes.length < 4) return ''
  const b = i => bytes[i] || 0
  const firstText = bytesToStr(bytes, 0, Math.min(bytes.length, 8192)).trimStart()

  if (b(0) === 0x25 && b(1) === 0x50 && b(2) === 0x44 && b(3) === 0x46) return 'application/pdf'
  if (b(0) === 0x89 && b(1) === 0x50 && b(2) === 0x4E && b(3) === 0x47) return 'image/png'
  if (b(0) === 0xFF && b(1) === 0xD8 && b(2) === 0xFF) return 'image/jpeg'
  if (b(0) === 0x47 && b(1) === 0x49 && b(2) === 0x46 && b(3) === 0x38) return 'image/gif'
  if (b(0) === 0x42 && b(1) === 0x4D) return 'image/bmp'
  if (b(0) === 0x00 && b(1) === 0x00 && b(2) === 0x01 && b(3) === 0x00) return 'image/x-icon'

  if (b(0) === 0x52 && b(1) === 0x49 && b(2) === 0x46 && b(3) === 0x46 && bytes.length > 11) {
    if (b(8) === 0x57 && b(9) === 0x45 && b(10) === 0x42 && b(11) === 0x50) return 'image/webp'
    if (b(8) === 0x41 && b(9) === 0x56 && b(10) === 0x49) return 'video/x-msvideo'
    if (b(8) === 0x57 && b(9) === 0x41 && b(10) === 0x56 && b(11) === 0x45) return 'audio/wav'
  }

  if (bytes.length > 11 && b(4) === 0x66 && b(5) === 0x74 && b(6) === 0x79 && b(7) === 0x70) {
    const brand = String.fromCharCode(b(8), b(9), b(10), b(11))
    if (brand === 'avif' || brand === 'avis') return 'image/avif'
    if (brand === 'heic' || brand === 'heix' || brand === 'heif' || brand === 'mif1' || brand === 'msf1') return 'image/heic'
    if (brand === 'qt  ') return 'video/quicktime'
    if (brand.startsWith('3g')) return 'video/3gpp'
    if (['M4A ', 'M4B ', 'f4a '].includes(brand)) return 'audio/mp4'
    return 'video/mp4'
  }

  if (b(0) === 0x1A && b(1) === 0x45 && b(2) === 0xDF && b(3) === 0xA3) {
    const ch = bytesToStr(bytes, 0, Math.min(bytes.length, 512)).toLowerCase()
    return ch.includes('webm') ? 'video/webm' : 'video/x-matroska'
  }

  if (b(0) === 0x46 && b(1) === 0x4C && b(2) === 0x56) return 'video/x-flv'
  if (b(0) === 0x49 && b(1) === 0x44 && b(2) === 0x33) return 'audio/mpeg'
  if (b(0) === 0xFF && (b(1) & 0xE0) === 0xE0) return 'audio/mpeg'
  if (b(0) === 0x66 && b(1) === 0x4C && b(2) === 0x61 && b(3) === 0x43) return 'audio/flac'

  if (b(0) === 0x4F && b(1) === 0x67 && b(2) === 0x67 && b(3) === 0x53) {
    const ch = bytesToStr(bytes, 0, Math.min(bytes.length, 512)).toLowerCase()
    if (ch.includes('theora')) return 'video/ogg'
    if (ch.includes('opus')) return 'audio/opus'
    return 'audio/ogg'
  }

  if (b(0) === 0x50 && b(1) === 0x4B && b(2) === 0x03 && b(3) === 0x04) {
    const ch = bytesToStr(bytes, 0, Math.min(bytes.length, 8192))
    if (ch.includes('AndroidManifest.xml') || ch.includes('classes.dex')) return 'application/vnd.android.package-archive'
    if (ch.includes('META-INF/MANIFEST.MF')) return 'application/java-archive'
    if (ch.includes('[Content_Types].xml')) {
      if (ch.includes('word/')) return 'application/vnd.openxmlformats-officedocument.wordprocessingml.document'
      if (ch.includes('xl/')) return 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
      if (ch.includes('ppt/')) return 'application/vnd.openxmlformats-officedocument.presentationml.presentation'
    }
    if (ch.includes('mimetypeapplication/epub+zip')) return 'application/epub+zip'
    return 'application/zip'
  }

  if (b(0) === 0x52 && b(1) === 0x61 && b(2) === 0x72 && b(3) === 0x21) return 'application/vnd.rar'
  if (b(0) === 0x37 && b(1) === 0x7A && b(2) === 0xBC && b(3) === 0xAF) return 'application/x-7z-compressed'
  if (b(0) === 0x1F && b(1) === 0x8B) return 'application/gzip'
  if (b(0) === 0x42 && b(1) === 0x5A && b(2) === 0x68) return 'application/x-bzip2'
  if (b(0) === 0xFD && b(1) === 0x37 && b(2) === 0x7A && b(3) === 0x58) return 'application/x-xz'
  if (b(0) === 0xD0 && b(1) === 0xCF && b(2) === 0x11 && b(3) === 0xE0) return 'application/msword'

  if (firstText.startsWith('<svg') || firstText.includes('<svg')) return 'image/svg+xml'
  if (/^<!doctype html/i.test(firstText) || /^<html/i.test(firstText)) return 'text/html'
  if (/^<\?xml/i.test(firstText)) return 'application/xml'
  if (firstText.startsWith('{') || firstText.startsWith('[')) return 'application/json'
  if (firstText.startsWith('#EXTM3U')) return 'application/vnd.apple.mpegurl'
  return ''
}

function bytesToStr(bytes, s, e) {
  let r = ''
  for (let i = s; i < e; i++) r += String.fromCharCode(bytes[i])
  return r
}

function parseSize(headers) {
  const cr = headers.get('content-range')
  if (cr) {
    const m = cr.match(/bytes\s+\d+-\d+\/(\d+)/i)
    if (m && m[1] && m[1] !== '*') {
      const n = parseInt(m[1], 10)
      if (n > 0) return n
    }
  }
  const cl = headers.get('content-length')
  if (cl) {
    const n = parseInt(cl, 10)
    if (n > 0) return n
  }
  return null
}

function parseMime(c) { return c ? String(c).split(';')[0].trim().toLowerCase() : '' }

function normalizeMime(m) {
  if (!m) return ''
  m = String(m).split(';')[0].trim().toLowerCase()
  const aliases = {
    'application/x-rar': 'application/vnd.rar',
    'application/x-rar-compressed': 'application/vnd.rar',
    'application/rar': 'application/vnd.rar',
    'audio/mp3': 'audio/mpeg',
    'audio/x-mp3': 'audio/mpeg',
    'video/x-mkv': 'video/x-matroska',
    'application/x-mpegurl': 'application/vnd.apple.mpegurl',
    'audio/x-mpegurl': 'application/vnd.apple.mpegurl',
    'application/vnd.apple.mpegurl.audio': 'application/vnd.apple.mpegurl',
    'text/javascript': 'application/javascript',
    'application/x-javascript': 'application/javascript',
    'image/jpg': 'image/jpeg',
    'binary/octet-stream': 'application/octet-stream'
  }
  return aliases[m] || m
}

function isWeakMime(m) {
  if (!m) return true
  m = normalizeMime(m)
  return m === 'application/octet-stream' || m === 'binary/octet-stream' || m === 'text/plain'
}

function extractFilename(cd) {
  if (!cd) return ''
  let m = cd.match(/filename\*\s*=\s*UTF-8''([^;\s]+)/i)
  if (m && m[1]) { try { return decodeURIComponent(m[1]).trim() } catch (_) {} }
  m = cd.match(/filename\s*=\s*"([^"]+)"/i)
  if (m && m[1]) { try { return decodeURIComponent(m[1].trim()) } catch (_) { return m[1].trim() } }
  m = cd.match(/filename\s*=\s*([^;\s"]+)/i)
  if (m && m[1]) { try { return decodeURIComponent(m[1].trim()) } catch (_) { return m[1].trim() } }
  return ''
}

function extractFilenameFromUrl(u) {
  try {
    const url = new URL(u)
    const possibleParams = ['filename', 'file', 'name', 'title', 'fn', 'download', 'attachment']
    for (const key of possibleParams) {
      const v = url.searchParams.get(key)
      if (v && v.includes('.')) return decodeURIComponent(v).trim()
    }
    let p = url.pathname.split('/').pop() || ''
    p = decodeURIComponent(p).split('?')[0].split('#')[0].trim()
    return p
  } catch (_) {
    try { return decodeURIComponent(String(u).split('?')[0].split('#')[0].split('/').pop() || '') } catch (_) { return '' }
  }
}

function getExtensionFromFilename(name) {
  if (!name) return ''
  try { name = decodeURIComponent(String(name)) } catch (_) { name = String(name) }
  name = name.split('?')[0].split('#')[0]
  const base = name.split('/').pop() || ''
  const dot = base.lastIndexOf('.')
  if (dot <= 0 || dot >= base.length - 1) return ''
  return base.substring(dot).toLowerCase()
}

function hasExtension(n) { return !!getExtensionFromFilename(n) }
function getMimeFromFilename(name) { const ext = getExtensionFromFilename(name); return ext ? (EXTENSION_MIME_MAP[ext] || 'application/octet-stream') : '' }

const EXTENSION_MIME_MAP = {
  '.txt':'text/plain','.text':'text/plain','.log':'text/plain','.nfo':'text/plain','.diz':'text/plain','.csv':'text/csv','.tsv':'text/tab-separated-values','.md':'text/markdown','.markdown':'text/markdown','.html':'text/html','.htm':'text/html','.xhtml':'application/xhtml+xml','.css':'text/css','.ics':'text/calendar','.vcf':'text/vcard','.rtx':'text/richtext',
  '.js':'application/javascript','.mjs':'application/javascript','.cjs':'application/javascript','.jsx':'text/jsx','.ts':'application/typescript','.tsx':'text/tsx','.json':'application/json','.jsonl':'application/x-ndjson','.map':'application/json','.xml':'application/xml','.xsl':'application/xml','.xslt':'application/xml','.yaml':'application/yaml','.yml':'application/yaml','.toml':'application/toml','.ini':'text/plain','.conf':'text/plain','.cfg':'text/plain','.env':'text/plain','.properties':'text/plain','.sql':'application/sql','.graphql':'application/graphql','.gql':'application/graphql',
  '.py':'text/x-python','.pyw':'text/x-python','.java':'text/x-java-source','.c':'text/x-c','.h':'text/x-c','.cpp':'text/x-c++src','.cc':'text/x-c++src','.cxx':'text/x-c++src','.hpp':'text/x-c++hdr','.cs':'text/x-csharp','.go':'text/x-go','.rs':'text/x-rust','.rb':'text/x-ruby','.php':'application/x-httpd-php','.swift':'text/x-swift','.kt':'text/x-kotlin','.kts':'text/x-kotlin','.dart':'text/x-dart','.lua':'text/x-lua','.pl':'text/x-perl','.pm':'text/x-perl','.r':'text/x-r','.sh':'application/x-sh','.bash':'application/x-sh','.zsh':'application/x-sh','.fish':'application/x-sh','.bat':'application/x-msdos-program','.cmd':'application/x-msdos-program','.ps1':'text/plain','.vue':'text/plain','.svelte':'text/plain',
  '.jpg':'image/jpeg','.jpeg':'image/jpeg','.jpe':'image/jpeg','.jfif':'image/jpeg','.png':'image/png','.apng':'image/apng','.gif':'image/gif','.webp':'image/webp','.bmp':'image/bmp','.dib':'image/bmp','.svg':'image/svg+xml','.svgz':'image/svg+xml','.ico':'image/x-icon','.cur':'image/x-icon','.tif':'image/tiff','.tiff':'image/tiff','.avif':'image/avif','.heic':'image/heic','.heif':'image/heif','.raw':'image/x-raw','.cr2':'image/x-canon-cr2','.nef':'image/x-nikon-nef','.orf':'image/x-olympus-orf','.psd':'image/vnd.adobe.photoshop','.ai':'application/postscript','.eps':'application/postscript',
  '.mp4':'video/mp4','.m4v':'video/x-m4v','.mp4v':'video/mp4','.webm':'video/webm','.mkv':'video/x-matroska','.mk3d':'video/x-matroska','.mov':'video/quicktime','.qt':'video/quicktime','.avi':'video/x-msvideo','.wmv':'video/x-ms-wmv','.asf':'video/x-ms-asf','.flv':'video/x-flv','.f4v':'video/mp4','.3gp':'video/3gpp','.3g2':'video/3gpp2','.mpeg':'video/mpeg','.mpg':'video/mpeg','.mpe':'video/mpeg','.m1v':'video/mpeg','.m2v':'video/mpeg','.vob':'video/dvd','.ogv':'video/ogg','.m2ts':'video/mp2t','.mts':'video/mp2t','.m3u8':'application/vnd.apple.mpegurl','.m3u':'audio/x-mpegurl','.mpd':'application/dash+xml',
  '.mp3':'audio/mpeg','.mpga':'audio/mpeg','.m4a':'audio/mp4','.m4b':'audio/mp4','.m4p':'audio/mp4','.aac':'audio/aac','.adts':'audio/aac','.ogg':'audio/ogg','.oga':'audio/ogg','.opus':'audio/opus','.wav':'audio/wav','.wave':'audio/wav','.weba':'audio/webm','.flac':'audio/flac','.alac':'audio/alac','.aiff':'audio/aiff','.aif':'audio/aiff','.aifc':'audio/aiff','.mid':'audio/midi','.midi':'audio/midi','.kar':'audio/midi','.amr':'audio/amr','.awb':'audio/amr-wb','.wma':'audio/x-ms-wma','.ra':'audio/vnd.rn-realaudio','.ram':'audio/vnd.rn-realaudio','.ape':'audio/ape','.mka':'audio/x-matroska','.ac3':'audio/ac3','.eac3':'audio/eac3','.dts':'audio/vnd.dts','.pls':'audio/x-scpls',
  '.pdf':'application/pdf','.doc':'application/msword','.dot':'application/msword','.docx':'application/vnd.openxmlformats-officedocument.wordprocessingml.document','.dotx':'application/vnd.openxmlformats-officedocument.wordprocessingml.template','.docm':'application/vnd.ms-word.document.macroEnabled.12','.xls':'application/vnd.ms-excel','.xlt':'application/vnd.ms-excel','.xlsx':'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet','.xlsm':'application/vnd.ms-excel.sheet.macroEnabled.12','.xltx':'application/vnd.openxmlformats-officedocument.spreadsheetml.template','.ppt':'application/vnd.ms-powerpoint','.pps':'application/vnd.ms-powerpoint','.pptx':'application/vnd.openxmlformats-officedocument.presentationml.presentation','.ppsx':'application/vnd.openxmlformats-officedocument.presentationml.slideshow','.pptm':'application/vnd.ms-powerpoint.presentation.macroEnabled.12','.odt':'application/vnd.oasis.opendocument.text','.ott':'application/vnd.oasis.opendocument.text-template','.ods':'application/vnd.oasis.opendocument.spreadsheet','.ots':'application/vnd.oasis.opendocument.spreadsheet-template','.odp':'application/vnd.oasis.opendocument.presentation','.otp':'application/vnd.oasis.opendocument.presentation-template','.odg':'application/vnd.oasis.opendocument.graphics','.rtf':'application/rtf','.epub':'application/epub+zip','.mobi':'application/x-mobipocket-ebook','.azw':'application/vnd.amazon.ebook','.azw3':'application/vnd.amazon.ebook','.fb2':'application/xml',
  '.zip':'application/zip','.zipx':'application/zip','.rar':'application/vnd.rar','.7z':'application/x-7z-compressed','.gz':'application/gzip','.gzip':'application/gzip','.tar':'application/x-tar','.tgz':'application/gzip','.bz2':'application/x-bzip2','.tbz':'application/x-bzip2','.tbz2':'application/x-bzip2','.xz':'application/x-xz','.txz':'application/x-xz','.lz':'application/x-lzip','.lzma':'application/x-lzma','.zst':'application/zstd','.cab':'application/vnd.ms-cab-compressed','.arj':'application/x-arj','.ace':'application/x-ace-compressed','.iso':'application/x-iso9660-image','.img':'application/octet-stream',
  '.apk':'application/vnd.android.package-archive','.apks':'application/octet-stream','.xapk':'application/octet-stream','.aab':'application/octet-stream','.ipa':'application/octet-stream','.exe':'application/vnd.microsoft.portable-executable','.msi':'application/x-msdownload','.msp':'application/x-msdownload','.msu':'application/x-msdownload','.dmg':'application/x-apple-diskimage','.pkg':'application/octet-stream','.deb':'application/vnd.debian.binary-package','.rpm':'application/x-rpm','.appimage':'application/octet-stream','.run':'application/octet-stream','.bin':'application/octet-stream',
  '.jar':'application/java-archive','.war':'application/java-archive','.ear':'application/java-archive','.class':'application/java-vm','.dex':'application/octet-stream','.wasm':'application/wasm','.dll':'application/octet-stream','.so':'application/octet-stream','.dylib':'application/octet-stream','.o':'application/octet-stream','.obj':'application/octet-stream',
  '.ttf':'font/ttf','.otf':'font/otf','.woff':'font/woff','.woff2':'font/woff2','.eot':'application/vnd.ms-fontobject',
  '.srt':'application/x-subrip','.vtt':'text/vtt','.ass':'text/plain','.ssa':'text/plain','.sub':'text/plain',
  '.torrent':'application/x-bittorrent','.sqlite':'application/vnd.sqlite3','.sqlite3':'application/vnd.sqlite3','.db':'application/octet-stream','.bak':'application/octet-stream','.backup':'application/octet-stream','.part':'application/octet-stream'
}

function getExtensionFromMime(m) {
  if (!m) return '.bin'
  m = normalizeMime(m)
  const map = {
    'text/plain':'.txt','text/csv':'.csv','text/tab-separated-values':'.tsv','text/markdown':'.md','text/html':'.html','text/css':'.css','text/calendar':'.ics','text/vcard':'.vcf','text/vtt':'.vtt','application/javascript':'.js','application/typescript':'.ts','application/json':'.json','application/x-ndjson':'.jsonl','application/xml':'.xml','application/xhtml+xml':'.xhtml','application/yaml':'.yaml','application/toml':'.toml','application/sql':'.sql','application/graphql':'.graphql',
    'image/jpeg':'.jpg','image/png':'.png','image/apng':'.apng','image/gif':'.gif','image/webp':'.webp','image/bmp':'.bmp','image/svg+xml':'.svg','image/x-icon':'.ico','image/tiff':'.tiff','image/avif':'.avif','image/heic':'.heic','image/heif':'.heif','image/vnd.adobe.photoshop':'.psd',
    'video/mp4':'.mp4','video/x-m4v':'.m4v','video/webm':'.webm','video/x-matroska':'.mkv','video/quicktime':'.mov','video/x-msvideo':'.avi','video/x-ms-wmv':'.wmv','video/x-flv':'.flv','video/3gpp':'.3gp','video/3gpp2':'.3g2','video/mpeg':'.mpeg','video/mp2t':'.ts','video/ogg':'.ogv','application/vnd.apple.mpegurl':'.m3u8','application/dash+xml':'.mpd',
    'audio/mpeg':'.mp3','audio/mp4':'.m4a','audio/aac':'.aac','audio/ogg':'.ogg','audio/opus':'.opus','audio/wav':'.wav','audio/webm':'.weba','audio/flac':'.flac','audio/aiff':'.aiff','audio/midi':'.mid','audio/amr':'.amr','audio/amr-wb':'.awb','audio/x-ms-wma':'.wma',
    'application/pdf':'.pdf','application/msword':'.doc','application/vnd.openxmlformats-officedocument.wordprocessingml.document':'.docx','application/vnd.ms-excel':'.xls','application/vnd.openxmlformats-officedocument.spreadsheetml.sheet':'.xlsx','application/vnd.ms-powerpoint':'.ppt','application/vnd.openxmlformats-officedocument.presentationml.presentation':'.pptx','application/vnd.oasis.opendocument.text':'.odt','application/vnd.oasis.opendocument.spreadsheet':'.ods','application/vnd.oasis.opendocument.presentation':'.odp','application/rtf':'.rtf','application/epub+zip':'.epub',
    'application/zip':'.zip','application/vnd.rar':'.rar','application/x-7z-compressed':'.7z','application/gzip':'.gz','application/x-tar':'.tar','application/x-bzip2':'.bz2','application/x-xz':'.xz','application/x-iso9660-image':'.iso','application/vnd.android.package-archive':'.apk','application/vnd.microsoft.portable-executable':'.exe','application/x-msdownload':'.exe','application/x-apple-diskimage':'.dmg','application/vnd.debian.binary-package':'.deb','application/x-rpm':'.rpm','application/java-archive':'.jar','application/java-vm':'.class','application/wasm':'.wasm','font/ttf':'.ttf','font/otf':'.otf','font/woff':'.woff','font/woff2':'.woff2','application/vnd.ms-fontobject':'.eot','application/x-bittorrent':'.torrent','application/vnd.sqlite3':'.sqlite','application/octet-stream':'.bin'
  }
  if (map[m]) return map[m]
  if (m.startsWith('video/')) return '.mp4'
  if (m.startsWith('audio/')) return '.mp3'
  if (m.startsWith('image/')) return '.' + m.split('/')[1].replace('+xml', '')
  if (m.startsWith('text/')) return '.txt'
  return '.bin'
}

function generateFilename(mid, h, mime) { return `${mimeToPrefix(mime)}-${String(h || mid || 'file').substring(0, 8)}${getExtensionFromMime(mime) || '.bin'}` }
function mimeToPrefix(m) {
  if (!m) return 'file'
  m = normalizeMime(m)
  if (m.startsWith('video/')) return 'video'
  if (m.startsWith('audio/')) return 'audio'
  if (m.startsWith('image/')) return 'image'
  if (m.includes('pdf') || m.includes('word') || m.includes('excel') || m.includes('presentation')) return 'document'
  if (m.includes('android') || m.includes('portable-executable') || m.includes('msdownload')) return 'app'
  if (m.includes('zip') || m.includes('rar') || m.includes('7z') || m.includes('gzip') || m.includes('tar')) return 'archive'
  if (m.startsWith('text/') || m.includes('javascript') || m.includes('typescript') || m.includes('json') || m.includes('xml')) return 'code'
  return 'file'
}

function sanitizeFilenameForHeader(n) {
  if (!n) return 'file.bin'
  return String(n).replace(/[<>:"/\\|?*\x00-\x1F]/g, '_').replace(/\s+/g, ' ').replace(/_{2,}/g, '_').trim().substring(0, 180)
}
function normalizePlayer(p) { p = String(p || '').toLowerCase().trim(); return ['plyr','videojs','art','hlsjs'].includes(p) ? p : p === 'vjs' ? 'videojs' : p === 'artplayer' ? 'art' : p === 'hls' ? 'hlsjs' : 'plyr' }
function formatBytes(b) { b = Number(b || 0); if (!b) return 'Unknown size'; const u=['B','KB','MB','GB','TB','PB']; let i=0; while(b>=1024&&i<u.length-1){b/=1024;i++} return b.toFixed(i?2:0)+' '+u[i] }
function getFileIcon(m) { if(!m)return'📦'; m=normalizeMime(m); if(m.startsWith('video/'))return'🎬'; if(m.startsWith('audio/'))return'🎵'; if(m.startsWith('image/'))return'🖼️'; if(m.includes('pdf'))return'📄'; if(m.includes('word'))return'📝'; if(m.includes('excel')||m.includes('spreadsheet'))return'📊'; if(m.includes('powerpoint')||m.includes('presentation'))return'📽️'; if(m.includes('zip')||m.includes('rar')||m.includes('7z')||m.includes('gzip')||m.includes('tar'))return'🗜️'; if(m.includes('android')||m.includes('portable-executable')||m.includes('msdownload'))return'📱'; if(m.startsWith('text/')||m.includes('javascript')||m.includes('typescript')||m.includes('json')||m.includes('xml')||m.includes('sql'))return'💻'; if(m.startsWith('font/'))return'🔤'; return'📦' }
function corsHeaders() { return { 'Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'GET,HEAD,OPTIONS','Access-Control-Allow-Headers':'Range,Content-Type','Access-Control-Max-Age':'86400' } }
function htmlResponse(html, headers = {}) { return new Response(html, { headers: { 'Content-Type':'text/html; charset=utf-8', ...headers } }) }
function escapeHtml(s){ if(!s)return''; return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;').replace(/'/g,'&#39;') }
function escapeAttr(s){ if(!s)return''; return String(s).replace(/&/g,'&amp;').replace(/"/g,'&quot;').replace(/</g,'&lt;').replace(/>/g,'&gt;') }
function safeText(s,m){ if(!s)return''; const t=String(s).replace(/[<>"|?\\/]/g,' '); return m&&t.length>m?t.substring(0,m):t }

function googleAnalyticsScript() {
  return `<script async src="https://www.googletagmanager.com/gtag/js?id=G-XPCZ4QGYJT"><\/script>
<script>
window.dataLayer = window.dataLayer || [];
function gtag(){dataLayer.push(arguments);}
gtag('js', new Date());
gtag('config', 'G-XPCZ4QGYJT');
<\/script>`
}

function homePageHtml() {
  return `<!DOCTYPE html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><meta name="theme-color" content="#e50914"><title>Host Wave</title>
<script src="https://telegram.org/js/telegram-web-app.js"></script>${googleAnalyticsScript()}
<style>*{margin:0;padding:0;box-sizing:border-box}body{background:#0f0f0f;color:#fff;font-family:system-ui,-apple-system,sans-serif;min-height:100vh;display:flex;flex-direction:column}header{padding:28px 20px;background:linear-gradient(135deg,#e50914,#b80711);text-align:center}header h1{font-size:clamp(2rem,8vw,3.2rem);font-weight:900}header p{color:rgba(255,255,255,.8);margin-top:8px}main{flex:1;max-width:900px;margin:0 auto;padding:30px 20px;text-align:center}.hero{background:rgba(255,255,255,.03);border:1px solid #2b2b2b;border-radius:20px;padding:40px 24px;margin-bottom:28px}.hero h2{font-size:clamp(1.3rem,5vw,2rem);margin-bottom:12px}.hero p{color:#aaa;line-height:1.7;max-width:560px;margin:0 auto}.cta{display:inline-block;background:linear-gradient(135deg,#e50914,#ff3340);color:#fff;padding:15px 36px;font-weight:900;border-radius:50px;text-decoration:none;margin-top:22px;box-shadow:0 10px 30px rgba(229,9,20,.4)}.grid{display:grid;grid-template-columns:repeat(3,1fr);gap:12px;margin-top:24px}.card{background:#141414;border:1px solid #222;border-radius:14px;padding:22px 14px;font-weight:700;color:#ddd}.card .icon{font-size:2rem;margin-bottom:8px}footer{padding:20px;font-size:.82rem;color:#555;text-align:center;border-top:1px solid #1a1a1a}@media(max-width:600px){.grid{grid-template-columns:1fr 1fr}}</style></head><body><header><h1>🌊 Host Wave</h1><p>Instant Telegram File Streaming</p></header><main><div class="hero"><h2>Stream Any File Instantly</h2><p>Send any file to our bot and get a shareable streaming link in seconds. Supports video, audio, images, apps, compressed files, text and code.</p><a href="https://t.me/Hostwave_bot" target="_blank" class="cta">▶ Open @Hostwave_bot</a></div><div class="grid"><div class="card"><div class="icon">⚡</div>Fast streaming</div><div class="card"><div class="icon">📱</div>4 players</div><div class="card"><div class="icon">📥</div>Direct download</div><div class="card"><div class="icon">🎬</div>Video & Audio</div><div class="card"><div class="icon">🖼️</div>Images</div><div class="card"><div class="icon">📦</div>Any file</div></div></main><footer>Host Wave © 2026</footer></body></html>`
}

function buildPlayerHtml(v) {
  const T = escapeHtml(v.safeTitle)
  const mJ = JSON.stringify(v.realMime || 'application/octet-stream')
  const fJ = JSON.stringify(v.realFilename || 'file')
  const puJ = JSON.stringify(v.pageUrl || '')
  const sz = v.contentLength ? formatBytes(v.contentLength) : 'Unknown size'
  const pl = v.player || 'plyr'
  const pa = v.pathname || ''
  const isV = v.realMime && (v.realMime.startsWith('video/') || v.realMime === 'application/vnd.apple.mpegurl' || v.realMime === 'application/dash+xml')
  const isA = v.realMime && v.realMime.startsWith('audio/')
  const isI = v.realMime && v.realMime.startsWith('image/')
  const isCode = isTextOrCode(v.realMime)
  const isM = isV || isA
  const sw = isM ? buildPlayerSwitcher(pa, pl, v.qCh, v.qFn, v.qMt, v.qFs) : ''
  const as = isM ? playerAssets(pl) : ''
  const pe = isM ? buildPlayerElement(pl, v.streamUrl, v.realMime, isV, isA) : ''
  const initS = isM ? playerInitScript(pl, v.streamUrl, v.realMime, v.realFilename, isV) : ''
  let ms = ''
  if (isM) ms = `<div class="pw"><div class="vc" id="vc"><div class="sp" id="sp"><div class="spin"></div></div>${pe}<div id="go" class="go"></div></div></div>`
  else if (isI) ms = `<div class="iv"><img id="vImg" src="${escapeAttr(v.streamUrl)}" alt="${T}" onload="this.parentNode.querySelector('.sp').style.display='none'" onerror="this.parentNode.querySelector('.sp').style.display='none'"><div class="sp"><div class="spin"></div></div></div>`
  else if (isCode) ms = `<div class="fb"><div style="font-size:3.2rem;margin-bottom:10px">${getFileIcon(v.realMime)}</div><h3 style="word-break:break-word">${T}</h3><p style="color:#777;margin-top:6px;font-size:.82rem">${escapeHtml(sz)}</p><a class="miniOpen" href="${escapeAttr(v.streamUrl)}" target="_blank">↗ Open inline</a></div>`
  else ms = `<div class="fb"><div style="font-size:3.2rem;margin-bottom:10px">${getFileIcon(v.realMime)}</div><h3 style="word-break:break-word">${T}</h3><p style="color:#666;margin-top:6px;font-size:.82rem">${escapeHtml(sz)}</p></div>`

  return `<!DOCTYPE html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1,user-scalable=no"><meta name="theme-color" content="#e50914"><title>${T}</title>
<script src="https://telegram.org/js/telegram-web-app.js"></script>${googleAnalyticsScript()}${as}
<style>*{margin:0;padding:0;box-sizing:border-box;-webkit-tap-highlight-color:transparent}html,body{height:100%;overflow-x:hidden}body{background:#0f0f0f;color:#fff;font-family:system-ui,-apple-system,sans-serif}header{width:100%;padding:12px 0;background:linear-gradient(135deg,#e50914,#b80711);text-align:center;border-radius:0 0 18px 18px}header h1{font-size:clamp(1.3rem,5vw,2rem);font-weight:900}header h1 a{color:#fff;text-decoration:none}.fi{text-align:center;padding:10px 14px 4px;max-width:1080px;margin:0 auto}.fi h3{font-size:clamp(.82rem,3vw,1.02rem);font-weight:800;word-break:break-word;color:#eee}.fi .m{color:#666;font-size:.72rem;margin-top:2px}.ps{display:grid;grid-template-columns:repeat(4,1fr);gap:5px;max-width:1080px;margin:8px auto;padding:0 10px}.ps a{background:#181818;color:#bbb;text-decoration:none;padding:7px 4px;border-radius:9px;border:1px solid #282828;font-weight:800;font-size:.7rem;text-align:center;display:flex;flex-direction:column;align-items:center;gap:1px}.ps a .i{font-size:1.1rem}.ps a:hover{border-color:#e50914}.ps a.ac{background:linear-gradient(135deg,#e50914,#ff3340);border-color:#e50914;color:#fff}.pw{width:100%;max-width:1080px;margin:6px auto 0}.vc{position:relative;width:100%;aspect-ratio:16/9;background:#000;overflow:hidden;border-radius:8px}.sp{position:absolute;inset:0;display:flex;align-items:center;justify-content:center;background:#000;z-index:5;transition:opacity .3s}.sp.h{opacity:0;pointer-events:none}.spin{width:40px;height:40px;border:4px solid rgba(255,255,255,.08);border-top-color:#e50914;border-radius:50%;animation:spn .8s linear infinite}@keyframes spn{to{transform:rotate(360deg)}}.vc video,.vc #art-player,.vc #hls-video{position:absolute;inset:0;width:100%!important;height:100%!important;background:#000;display:block;object-fit:contain}.plyr{position:absolute!important;inset:0!important;width:100%!important;height:100%!important}.plyr video{object-fit:contain}.plyr__volume input[type=range]{display:none!important}.plyr__volume{min-width:0!important;width:auto!important}.video-js{position:absolute!important;inset:0!important;width:100%!important;height:100%!important}.vjs-default-skin .vjs-big-play-button{left:50%!important;top:50%!important;transform:translate(-50%,-50%)!important;border-radius:50%!important;width:52px!important;height:52px!important;line-height:52px!important;border:none!important}.vjs-volume-bar,.vjs-volume-level{display:none!important}.vjs-mute-control{display:inline-block!important}#art-player{border-radius:0!important}.art-volume-slider{display:none!important}.aw{display:flex;align-items:center;justify-content:center;width:100%;padding:30px 16px;background:#111;flex-direction:column;gap:14px;border-radius:8px}.aw .ai{font-size:3.5rem}.aw audio{width:100%;max-width:420px}.iv{width:100%;max-width:1080px;margin:8px auto;background:#000;display:flex;align-items:center;justify-content:center;min-height:240px;position:relative;overflow:hidden;border-radius:8px}.iv img{max-width:100%;max-height:78vh;object-fit:contain}.fb{background:#141414;border:1px solid #222;border-radius:14px;padding:36px 20px;text-align:center;width:96%;max-width:1080px;margin:8px auto}.miniOpen{display:inline-block;margin-top:14px;background:#202020;border:1px solid #333;color:#eee;text-decoration:none;border-radius:12px;padding:10px 18px;font-weight:800}.go{position:absolute;inset:0;display:flex;align-items:center;justify-content:center;color:#fff;font-size:1.7rem;font-weight:900;background:rgba(0,0,0,.4);opacity:0;transition:opacity .18s;pointer-events:none;z-index:10}.go.s{opacity:1}.tf{position:absolute;color:#fff;font-size:.88rem;font-weight:900;transform:translate(-50%,-50%);pointer-events:none;animation:ta .6s ease-out forwards;z-index:20}@keyframes ta{from{opacity:1;transform:translate(-50%,-50%) scale(1)}to{opacity:0;transform:translate(-50%,-60%) scale(1.4)}}.cta-b{max-width:520px;margin:14px auto 6px;background:linear-gradient(135deg,#1a1a2e,#16213e 50%,#0f3460);border:1px solid #2a2a4a;border-radius:16px;padding:18px 20px;display:flex;align-items:center;gap:14px;text-decoration:none;color:#fff}.cta-b:hover{border-color:#e50914}.dw{display:flex;flex-direction:column;align-items:center;max-width:1080px;margin:12px auto 6px;padding:0 10px;gap:9px}.cp{display:flex;width:100%;max-width:520px;background:#1a1a1a;border:1px solid #2a2a2a;border-radius:14px;padding:12px 18px;align-items:center;justify-content:center;gap:10px;color:#888;font-size:.84rem;font-weight:700}.cr{width:28px;height:28px;position:relative;flex-shrink:0}.cr svg{width:28px;height:28px;transform:rotate(-90deg)}.cr circle{fill:none;stroke:#e50914;stroke-width:3;stroke-dasharray:82;stroke-dashoffset:0;transition:stroke-dashoffset 1s linear;stroke-linecap:round}.cn{position:absolute;inset:0;display:flex;align-items:center;justify-content:center;font-size:.68rem;font-weight:900;color:#e50914}.db{display:none;width:100%;max-width:520px;background:linear-gradient(135deg,#e50914,#ff3340 60%,#ff6b35);color:#fff;border:none;border-radius:14px;padding:14px 20px;font-size:1rem;font-weight:900;cursor:pointer;box-shadow:0 6px 28px rgba(229,9,20,.4);text-decoration:none;text-align:center;position:relative;overflow:hidden}.db:active{transform:scale(.97)}.db .shm{position:absolute;top:0;left:-120%;width:60%;height:100%;background:linear-gradient(90deg,transparent,rgba(255,255,255,.2),transparent);animation:shm 2.8s infinite}@keyframes shm{0%{left:-120%}55%,100%{left:130%}}.dok{display:none;width:100%;max-width:520px;background:#0d2818;border:1px solid #1a5c2e;border-radius:12px;padding:12px 16px;text-align:center;font-size:.82rem;color:#81c784}.cw{width:96%;max-width:1080px;margin:14px auto}.cw h3{color:#e50914;font-size:.9rem;margin-bottom:8px;text-align:center;font-weight:900}.cl{display:flex;flex-direction:column;gap:6px;max-height:400px;overflow-y:auto;padding:8px;background:#111;border-radius:12px;-webkit-overflow-scrolling:touch}.ci{background:#1a1a1a;border:1px solid #222;border-radius:10px;display:flex;align-items:center;gap:10px;padding:10px 12px;cursor:pointer;text-decoration:none;color:#fff;position:relative;overflow:hidden;min-height:56px;transition:border-color .15s}.ci:hover{border-color:#e50914}.ci-ic{width:40px;height:40px;border-radius:8px;background:linear-gradient(135deg,#e50914,#ff3340);display:flex;align-items:center;justify-content:center;font-size:1.1rem;flex-shrink:0}.ci-bd{flex:1;min-width:0;display:flex;flex-direction:column;gap:4px}.ci-nm{font-size:.8rem;font-weight:700;color:#eee;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}.ci-br{width:100%;height:4px;background:#2a2a2a;border-radius:4px;overflow:hidden}.ci-fl{height:100%;background:linear-gradient(90deg,#e50914,#ff6b35);border-radius:4px;min-width:2px}.ci-mt{font-size:.68rem;color:#777;display:flex;gap:8px;flex-wrap:wrap}.ci-pl{width:32px;height:32px;border-radius:50%;background:rgba(229,9,20,.15);border:1px solid rgba(229,9,20,.3);display:flex;align-items:center;justify-content:center;flex-shrink:0;font-size:.75rem}.ci:hover .ci-pl{background:rgba(229,9,20,.4)}.ci-x{position:absolute;top:4px;right:6px;background:none;border:none;color:#444;cursor:pointer;font-size:.72rem;padding:3px 5px;border-radius:4px;z-index:2;line-height:1}.ci-x:hover{color:#e50914}.ci-empty{text-align:center;color:#555;font-size:.8rem;padding:20px 8px}footer{text-align:center;padding:14px 0;color:#555;font-size:.78rem;background:#0a0a0a;border-top:1px solid #1a1a1a;margin-top:14px}@media(max-width:480px){.ps{grid-template-columns:repeat(2,1fr)}.cta-b{margin:10px 10px 6px}.ci{padding:8px 10px;min-height:50px}.ci-ic{width:36px;height:36px;font-size:1rem}.ci-nm{font-size:.75rem}.ci-pl{width:28px;height:28px;font-size:.65rem}}</style></head><body><header><h1><a href="/">🌊 Host Wave</a></h1></header><div class="fi"><h3>${T}</h3><div class="m">${escapeHtml(v.realMime)} | ${escapeHtml(sz)}</div></div>${sw}${ms}<a class="cta-b" href="https://t.me/Hostwave_bot" target="_blank"><span style="font-size:2.2rem">☁️</span><span style="flex:1;min-width:0"><span style="font-weight:900;font-size:.92rem;display:block">Store your files on Host Wave</span><span style="font-size:.72rem;color:#8892b0">Videos, music, docs — instant links from Telegram</span></span><span style="color:#e50914;font-size:1.2rem">›</span></a><div class="dw"><div class="cp" id="cdP"><span style="opacity:.4">⬇</span><div class="cr"><svg viewBox="0 0 30 30"><circle cx="15" cy="15" r="13" id="cdC"/></svg><div class="cn" id="cdN">15</div></div><span>Download in <b id="cdS">15</b>s</span></div><a class="db" id="dlB" href="${escapeAttr(v.downloadUrl)}" download="${escapeAttr(v.realFilename)}" style="display:none">⬇ Download ${escapeHtml(safeText(v.realFilename, 40))} (${escapeHtml(sz)})<div class="shm"></div></a><div class="dok" id="dlOk">✅ Download started</div></div>${adsterraBlock()}<div class="cw"><h3>▶ Continue Watching</h3><div class="cl" id="cwList"></div></div><footer>Host Wave © 2026 · <a href="https://t.me/Hostwave_bot" target="_blank" style="color:#e50914">@Hostwave_bot</a></footer>${initS}${clientScript(mJ, fJ, puJ)}</body></html>`
}

function adsterraBlock() {
  return `<div style="margin:16px auto;max-width:1080px;display:flex;align-items:center;justify-content:center;min-height:260px">
<script type="text/javascript">
atOptions = {
  'key' : 'fda92a323feb4e7f00bdabfc7dbfbb48',
  'format' : 'iframe',
  'height' : 250,
  'width' : 300,
  'params' : {}
};
<\/script>
<script type="text/javascript" src="https://www.highperformanceformat.com/fda92a323feb4e7f00bdabfc7dbfbb48/invoke.js"><\/script>
</div>`
}

function isTextOrCode(m) { if(!m)return false; m=normalizeMime(m); return m.startsWith('text/')||m.includes('javascript')||m.includes('typescript')||m.includes('json')||m.includes('xml')||m.includes('yaml')||m.includes('toml')||m.includes('sql')||m.includes('graphql') }

function buildPlayerSwitcher(pa, ac, qCh, qFn, qMt, qFs) {
  const ps = [['plyr','🎬','Plyr'],['videojs','▶️','Video.js'],['art','🎨','ArtPlayer'],['hlsjs','📡','HLS.js']]
  return '<div class="ps">' + ps.map(([k,i,l]) => {
    const p = new URLSearchParams()
    if (qCh) p.set('ch', qCh); if (qFn) p.set('fn', qFn); if (qMt) p.set('mt', qMt); if (qFs) p.set('fs', String(qFs)); p.set('player', k)
    return `<a class="${k===ac?'ac':''}" href="${escapeAttr(pa+'?'+p.toString())}"><span class="i">${i}</span><span>${escapeHtml(l)}</span></a>`
  }).join('') + '</div>'
}

function buildPlayerElement(pl, su, mi, isV, isA) {
  const s = escapeAttr(su), t = escapeAttr(mi)
  if (isA) return `<div class="aw"><div class="ai">🎵</div><audio id="media-player" controls preload="auto"><source src="${s}" type="${t}"></audio></div>`
  if (pl === 'plyr') return `<video id="media-player" controls playsinline preload="auto" crossorigin="anonymous"><source src="${s}" type="${t}"></video>`
  if (pl === 'videojs') return `<video id="vjs-player" class="video-js vjs-default-skin vjs-big-play-centered" controls preload="auto" playsinline crossorigin="anonymous"><source src="${s}" type="${t}"></video>`
  if (pl === 'art') return `<div id="art-player"></div>`
  if (pl === 'hlsjs') return `<video id="hls-video" controls playsinline preload="auto" style="width:100%;height:100%;background:#000"></video>`
  return `<video id="media-player" controls playsinline preload="auto"><source src="${s}" type="${t}"></video>`
}

function playerAssets(pl) {
  if (pl === 'plyr') return `<link rel="stylesheet" href="https://cdn.plyr.io/3.7.8/plyr.css"><script src="https://cdn.plyr.io/3.7.8/plyr.polyfilled.js"><\/script>`
  if (pl === 'videojs') return `<link href="https://vjs.zencdn.net/8.16.1/video-js.css" rel="stylesheet"><script src="https://vjs.zencdn.net/8.16.1/video.min.js"><\/script>`
  if (pl === 'art') return `<script src="https://cdn.jsdelivr.net/npm/artplayer@5/dist/artplayer.js"><\/script><script src="https://cdn.jsdelivr.net/npm/hls.js@latest/dist/hls.min.js"><\/script>`
  if (pl === 'hlsjs') return `<script src="https://cdn.jsdelivr.net/npm/hls.js@latest/dist/hls.min.js"><\/script>`
  return ''
}

function playerInitScript(pl, su, mi, fn, isV) {
  const sJ = JSON.stringify(su), mJ = JSON.stringify(mi), tJ = JSON.stringify(fn)
  if (!isV) return ''
  if (pl === 'plyr') return `<script>document.addEventListener('DOMContentLoaded',function(){try{var el=document.getElementById('media-player');if(!el)return;var p=new Plyr(el,{controls:['play-large','play','rewind','fast-forward','progress','current-time','duration','mute','settings','pip','fullscreen'],settings:['speed'],speed:{selected:1,options:[.5,.75,1,1.25,1.5,2]},keyboard:{focused:true,global:true},ratio:'16:9',invertTime:false});p.on('ready canplay',function(){var s=document.getElementById('sp');if(s)s.classList.add('h')})}catch(e){console.warn('Plyr:',e)}});<\/script>`
  if (pl === 'videojs') return `<script>document.addEventListener('DOMContentLoaded',function(){try{if(!document.getElementById('vjs-player'))return;var p=videojs('vjs-player',{controls:true,fluid:false,fill:true,preload:'auto',playbackRates:[.5,.75,1,1.25,1.5,2],html5:{vhs:{overrideNative:true},nativeAudioTracks:false,nativeVideoTracks:false},sources:[{src:${sJ},type:${mJ}}]});p.ready(function(){this.on('canplay',function(){var s=document.getElementById('sp');if(s)s.classList.add('h')});this.on('error',function(){var e=this.error();if(e&&e.code===4){this.src({src:${sJ},type:'video/mp4'});this.load()}})})}catch(e){console.warn('VJS:',e)}});<\/script>`
  if (pl === 'art') return `<script>document.addEventListener('DOMContentLoaded',function(){try{if(!window.Artplayer)return;var o={container:'#art-player',url:${sJ},title:${tJ},volume:.8,autoplay:false,pip:true,setting:true,playbackRate:true,fullscreen:true,fullscreenWeb:true,hotkey:true,mutex:true,theme:'#e50914'};var m=${mJ};if(m.indexOf('mpegurl')!==-1||m.indexOf('dash+xml')!==-1||${sJ}.indexOf('.m3u8')!==-1){o.type='m3u8';o.customType={m3u8:function(v,u){if(window.Hls&&Hls.isSupported()){var h=new Hls({maxBufferLength:30});h.loadSource(u);h.attachMedia(v)}else if(v.canPlayType('application/vnd.apple.mpegurl'))v.src=u}}}var a=new Artplayer(o);a.on('ready',function(){var s=document.getElementById('sp');if(s)s.classList.add('h')});a.on('video:canplay',function(){var s=document.getElementById('sp');if(s)s.classList.add('h')})}catch(e){console.warn('Art:',e)}});<\/script>`
  if (pl === 'hlsjs') return `<script>document.addEventListener('DOMContentLoaded',function(){try{var v=document.getElementById('hls-video');if(!v)return;var src=${sJ},m=${mJ},isH=m.indexOf('mpegurl')!==-1||src.indexOf('.m3u8')!==-1;if(isH&&window.Hls&&Hls.isSupported()){var h=new Hls({maxBufferLength:30,enableWorker:true,startLevel:-1});h.loadSource(src);h.attachMedia(v);h.on(Hls.Events.MANIFEST_PARSED,function(){var s=document.getElementById('sp');if(s)s.classList.add('h')});h.on(Hls.Events.ERROR,function(e,d){if(d.fatal){if(d.type===Hls.ErrorTypes.NETWORK_ERROR)h.startLoad();else if(d.type===Hls.ErrorTypes.MEDIA_ERROR)h.recoverMediaError();else h.destroy()}})}else{v.src=src;v.addEventListener('canplay',function(){var s=document.getElementById('sp');if(s)s.classList.add('h')},{once:true})}}catch(e){console.warn('HLS:',e)}});<\/script>`
  return ''
}

function clientScript(mJ, fJ, puJ) {
  return `<script>;(function(){'use strict';var M=${mJ},F=${fJ},PU=${puJ};var isV=M.startsWith('video/')||M.indexOf('mpegurl')!==-1||M.indexOf('dash+xml')!==-1,isA=M.startsWith('audio/'),isM=isV||isA;var CURRID=location.pathname.replace(/[^a-zA-Z0-9]/g,'_');function hS(){var s=document.getElementById('sp');if(s)s.classList.add('h')}var me=document.querySelector('video,audio');if(me){me.addEventListener('canplay',hS,{once:true});me.addEventListener('playing',hS,{once:true})}var TOT=15,sec=TOT,CIR=81.7,cdC=document.getElementById('cdC'),cdN=document.getElementById('cdN'),cdS=document.getElementById('cdS'),cdP=document.getElementById('cdP'),dlB=document.getElementById('dlB'),dlOk=document.getElementById('dlOk');if(cdC)cdC.style.strokeDasharray=CIR;function sR(s){if(cdC)cdC.style.strokeDashoffset=CIR-(s/TOT)*CIR}sR(TOT);var ci=setInterval(function(){sec--;if(sec>0){if(cdN)cdN.textContent=sec;if(cdS)cdS.textContent=sec;sR(sec)}else{clearInterval(ci);if(cdP)cdP.style.display='none';if(dlB)dlB.style.display='block'}},1000);if(dlB)dlB.addEventListener('click',function(){setTimeout(function(){if(dlOk)dlOk.style.display='block'},1500)});var HK='hw_watch_v2';function hGet(){try{return JSON.parse(localStorage.getItem(HK)||'[]')}catch(_){return[]}}function hSet(a){try{localStorage.setItem(HK,JSON.stringify(a.slice(0,50)))}catch(_){}}function hDel(id){hSet(hGet().filter(function(x){return x.id!==id}))}function hSave(t,d){if(!isM||!CURRID)return;var a=hGet(),found=false;for(var i=0;i<a.length;i++){if(a[i].id===CURRID){a[i].t=t;a[i].d=d||a[i].d;a[i].ts=Date.now();found=true;break}}if(!found)a.unshift({id:CURRID,n:F||'Unknown',u:PU,m:M,t:t,d:d||0,ts:Date.now()});a.sort(function(x,y){return y.ts-x.ts});hSet(a)}if(isM&&me){var items=hGet();for(var i=0;i<items.length;i++){if(items[i].id===CURRID&&items[i].t>5){var st=items[i].t;me.addEventListener('loadedmetadata',function(){me.currentTime=st},{once:true});break}}me.addEventListener('timeupdate',function(){if(!me.paused&&!isNaN(me.currentTime)&&me.currentTime>2)hSave(me.currentTime,me.duration)});me.addEventListener('pause',function(){if(!isNaN(me.currentTime))hSave(me.currentTime,me.duration)});me.addEventListener('ended',function(){hDel(CURRID);cwRender()})}function fT(s){if(!s||isNaN(s))return'0:00';s=Math.floor(s);var h=Math.floor(s/3600),m=Math.floor((s%3600)/60),ss=s%60;if(h>0)return h+':'+(m<10?'0':'')+m+':'+(ss<10?'0':'')+ss;return m+':'+(ss<10?'0':'')+ss}function eH(s){if(!s)return'';return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;')}function cwRender(){var list=document.getElementById('cwList');if(!list)return;var all=hGet().filter(function(x){return x.t>10&&x.id!==CURRID&&x.u});if(!all.length){list.innerHTML='<div class="ci-empty">No watch history yet</div>';return}list.innerHTML='';for(var i=0;i<all.length;i++){var it=all[i];var pct=it.d>0?Math.min(99.9,(it.t/it.d)*100).toFixed(1):'0';var ic=it.m&&it.m.startsWith('audio/')?'🎵':'🎬';var nm=it.n||'Unknown';if(nm.length>45)nm=nm.substring(0,42)+'...';var ts=fT(it.t);if(it.d>0)ts+=' / '+fT(it.d);var a=document.createElement('a');a.className='ci';a.href=it.u;a.innerHTML='<div class="ci-ic">'+ic+'</div><div class="ci-bd"><div class="ci-nm">'+eH(nm)+'</div><div class="ci-br"><div class="ci-fl" style="width:'+pct+'%"></div></div><div class="ci-mt"><span>'+ts+'</span><span>'+pct+'%</span></div></div><div class="ci-pl">▶</div><button class="ci-x" data-id="'+eH(it.id)+'">✕</button>';list.appendChild(a)}}cwRender();document.addEventListener('click',function(e){var btn=e.target;if(!btn.classList||!btn.classList.contains('ci-x'))return;e.preventDefault();e.stopPropagation();hDel(btn.getAttribute('data-id'));cwRender()});var vc=document.getElementById('vc'),goEl=document.getElementById('go'),goT;function sG(t){if(!goEl)return;goEl.textContent=t;goEl.classList.add('s');clearTimeout(goT);goT=setTimeout(function(){goEl.classList.remove('s')},900)}function tF(x,y,t){if(!vc)return;var fb=document.createElement('div');fb.className='tf';fb.style.left=x+'px';fb.style.top=y+'px';fb.textContent=t;vc.appendChild(fb);setTimeout(function(){fb.remove()},700)}function gM(){return document.querySelector('video,audio')}if(vc&&isM){var sX=0,sw=false;vc.addEventListener('touchstart',function(e){if(e.touches.length===1){sX=e.touches[0].clientX;sw=true}},{passive:true});vc.addEventListener('touchmove',function(e){var m=gM();if(!m||!sw||e.touches.length!==1)return;e.preventDefault();var dx=e.touches[0].clientX-sX;if(Math.abs(dx)>30){var sk=Math.min(60,Math.max(3,Math.abs(dx)/8));m.currentTime=dx>0?Math.min(m.duration||Infinity,m.currentTime+sk):Math.max(0,m.currentTime-sk);var p=function(n){return n.toString().padStart(2,'0')};var cm=Math.floor(m.currentTime/60),cs=Math.floor(m.currentTime%60),dm=Math.floor((m.duration||0)/60),ds=Math.floor((m.duration||0)%60);sG((dx>0?'⏩ ':'⏪ ')+p(cm)+':'+p(cs)+' / '+p(dm)+':'+p(ds));sX=e.touches[0].clientX}},{passive:false});vc.addEventListener('touchend',function(){sw=false});var lT=0;vc.addEventListener('click',function(e){var m=gM();if(!m)return;var r=vc.getBoundingClientRect(),x=e.clientX-r.left,y=e.clientY-r.top;if(y>r.height-50)return;var n=Date.now();if(n-lT>300){lT=n;return}lT=n;if(x<r.width*.35){m.currentTime=Math.max(0,m.currentTime-10);tF(x,y,'-10s')}else if(x>r.width*.65){m.currentTime=Math.min(m.duration||Infinity,m.currentTime+10);tF(x,y,'+10s')}});function hFS(){var f=!!(document.fullscreenElement||document.webkitFullscreenElement);if(f&&screen.orientation&&screen.orientation.lock)screen.orientation.lock('landscape').catch(function(){});else if(!f&&screen.orientation&&screen.orientation.unlock)screen.orientation.unlock()}document.addEventListener('fullscreenchange',hFS);document.addEventListener('webkitfullscreenchange',hFS)}})();<\/script>`
}
