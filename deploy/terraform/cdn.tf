# CloudFront in front of the edge service: the first line of defense
# against traffic spikes. Price and candle responses carry short
# Cache-Control lifetimes (1 s for live data, 60 s for closed candles), so
# a burst of identical requests reaches the origin about once per second
# per CloudFront location. WebSockets pass through uncached.

data "aws_cloudfront_cache_policy" "disabled" {
  name = "Managed-CachingDisabled"
}

data "aws_cloudfront_origin_request_policy" "all_viewer" {
  name = "Managed-AllViewer"
}

resource "aws_cloudfront_cache_policy" "market_data" {
  name    = "${local.name}-market-data"
  comment = "Honors origin Cache-Control; keys on the API's query parameters only"

  min_ttl     = 0
  default_ttl = 1
  max_ttl     = 60

  parameters_in_cache_key_and_forwarded_to_origin {
    enable_accept_encoding_gzip   = true
    enable_accept_encoding_brotli = true

    cookies_config {
      cookie_behavior = "none"
    }
    headers_config {
      header_behavior = "none"
    }
    # Only parameters the API reads, so junk parameters cannot be used to
    # bust the cache and send every request to the origin.
    query_strings_config {
      query_string_behavior = "whitelist"
      query_strings {
        items = ["markets", "interval", "limit", "end"]
      }
    }
  }
}

resource "aws_cloudfront_distribution" "main" {
  enabled         = true
  is_ipv6_enabled = true
  comment         = "Orderflow price feed"
  price_class     = "PriceClass_100"

  origin {
    origin_id   = "edge"
    domain_name = var.origin_domain_name

    custom_origin_config {
      http_port              = 80
      https_port             = 443
      origin_protocol_policy = "https-only"
      origin_ssl_protocols   = ["TLSv1.2"]
      # Long enough for WebSockets between pings.
      origin_read_timeout      = 60
      origin_keepalive_timeout = 60
    }

    custom_header {
      name  = "X-Origin-Verify"
      value = random_password.origin_verify.result
    }
  }

  # REST market data: cached briefly at the edge.
  default_cache_behavior {
    target_origin_id       = "edge"
    viewer_protocol_policy = "redirect-to-https"
    allowed_methods        = ["GET", "HEAD", "OPTIONS"]
    cached_methods         = ["GET", "HEAD"]
    cache_policy_id        = aws_cloudfront_cache_policy.market_data.id
    compress               = true
  }

  # WebSocket upgrades must reach the origin with their headers intact.
  ordered_cache_behavior {
    path_pattern             = "/v1/ws"
    target_origin_id         = "edge"
    viewer_protocol_policy   = "https-only"
    allowed_methods          = ["GET", "HEAD", "OPTIONS"]
    cached_methods           = ["GET", "HEAD"]
    cache_policy_id          = data.aws_cloudfront_cache_policy.disabled.id
    origin_request_policy_id = data.aws_cloudfront_origin_request_policy.all_viewer.id
  }

  # Health and metrics are for the load balancer and Prometheus only.
  ordered_cache_behavior {
    path_pattern           = "/metrics"
    target_origin_id       = "edge"
    viewer_protocol_policy = "https-only"
    allowed_methods        = ["GET", "HEAD"]
    cached_methods         = ["GET", "HEAD"]
    cache_policy_id        = data.aws_cloudfront_cache_policy.disabled.id
    function_association {
      event_type   = "viewer-request"
      function_arn = aws_cloudfront_function.deny.arn
    }
  }

  restrictions {
    geo_restriction {
      restriction_type = "none"
    }
  }

  viewer_certificate {
    cloudfront_default_certificate = true
  }
}

# Answers 404 at the edge for paths that must not be public.
resource "aws_cloudfront_function" "deny" {
  name    = "${local.name}-deny"
  runtime = "cloudfront-js-2.0"
  comment = "Hide internal endpoints"
  publish = true
  code    = <<-JS
    function handler(event) {
      return { statusCode: 404, statusDescription: 'Not Found' };
    }
  JS
}
