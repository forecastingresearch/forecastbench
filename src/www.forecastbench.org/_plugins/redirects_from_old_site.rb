Jekyll::Hooks.register :site, :post_write do |site|
  redirect_html = <<~HTML
    <!DOCTYPE html>
    <html>
    <head>
      <meta charset="utf-8">
      <title>Redirecting...</title>
      <meta http-equiv="refresh" content="0; url=/tournament/">
      <link rel="canonical" href="/tournament/">
      <script>window.location.replace("/tournament/");</script>
    </head>
    <body style="display:none;">
    </body>
    </html>
  HTML

  total_count = 0

  # Replace all HTML files in assets/iclr2025/leaderboards/ with redirects
  iclr_leaderboards_dir = File.join(site.dest, 'assets', 'iclr2025', 'leaderboards')
  if Dir.exist?(iclr_leaderboards_dir)
    html_files = Dir.glob(File.join(iclr_leaderboards_dir, '*.html'))
    html_files.each do |file_path|
      File.write(file_path, redirect_html)
    end
    total_count += html_files.length
    puts "Generated #{html_files.length} legacy leaderboard redirects in assets/iclr2025/leaderboards/"
  end

  # Create redirects in /leaderboards/ for legacy URLs
  leaderboards_dir = File.join(site.dest, 'leaderboards')
  FileUtils.mkdir_p(leaderboards_dir)

  legacy_files = [
    'human_combo_generated_leaderboard_30.html',
    'human_combo_generated_leaderboard_7.html',
    'human_combo_generated_leaderboard_overall.html',
    'human_combo_leaderboard_30.html',
    'human_combo_leaderboard_7.html',
    'human_combo_leaderboard_overall.html',
    'human_leaderboard_30.html',
    'human_leaderboard_7.html',
    'human_leaderboard_overall.html',
    'leaderboard_30.html',
    'leaderboard_7.html',
    'leaderboard_overall.html'
  ]

  legacy_files.each do |filename|
    file_path = File.join(leaderboards_dir, filename)
    File.write(file_path, redirect_html)
  end
  total_count += legacy_files.length
  puts "Generated #{legacy_files.length} legacy leaderboard redirects in /leaderboards/"

  # Create redirect for datasets.html -> /datasets/
  datasets_redirect = <<~HTML
    <!DOCTYPE html>
    <html>
    <head>
      <meta charset="utf-8">
      <title>Redirecting...</title>
      <meta http-equiv="refresh" content="0; url=/datasets/">
      <link rel="canonical" href="/datasets/">
      <script>window.location.replace("/datasets/");</script>
    </head>
    <body style="display:none;">
    </body>
    </html>
  HTML

  datasets_file = File.join(site.dest, 'datasets.html')
  File.write(datasets_file, datasets_redirect)
  total_count += 1

  datasets_forecast_file = File.join(site.dest, 'datasets_forecast_sets_index.html')
  File.write(datasets_forecast_file, datasets_redirect)
  total_count += 1

  puts "Generated redirects for datasets.html and datasets_forecast_sets_index.html"

  puts "Total redirects generated: #{total_count}"
end
