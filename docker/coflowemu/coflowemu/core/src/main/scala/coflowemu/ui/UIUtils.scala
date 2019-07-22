package coflowemu.ui

import scala.xml.Node

/** Utility functions for generating XML pages with coflowemu content. */
private[coflowemu] object UIUtils {
  import Page._

  // Yarn has to go through a proxy so the base uri is provided and has to be on all links
  private[coflowemu] val uiRoot : String = Option(System.getenv("APPLICATION_WEB_PROXY_BASE")).
    getOrElse("")

  def prependBaseUri(resource: String = "") = uiRoot + resource

  /** Returns a coflowemu page with correctly formatted headers */
  def headerVarysPage(content: => Seq[Node], title: String, page: Page.Value)
  : Seq[Node] = {
    val environment = page match {
      case Environment => 
        <li class="active"><a href={prependBaseUri("/environment")}>Environment</a></li>
      case _ => <li><a href={prependBaseUri("/environment")}>Environment</a></li>
    }

    <html>
      <head>
        <meta http-equiv="Content-type" content="text/html; charset=utf-8" />
        <link rel="stylesheet" href={prependBaseUri("/static/bootstrap.min.css")} type="text/css" />
        <link rel="stylesheet" href={prependBaseUri("/static/webui.css")}  type="text/css" />
        <script src={prependBaseUri("/static/sorttable.js")} ></script>
        <title>{title}</title>
      </head>
      <body>
        <div class="navbar navbar-static-top">
          <div class="navbar-inner">
            <a href={prependBaseUri("/")} class="brand"><img src={prependBaseUri("/static/coflowemu-logo-77x50px-hd.png")}  /></a>
            <ul class="nav">
              {environment}
            </ul>
            <p class="navbar-text pull-right"><strong>TBD</strong> UI</p>
          </div>
        </div>

        <div class="container-fluid">
          <div class="row-fluid">
            <div class="span12">
              <h3 style="vertical-align: bottom; display: inline-block;">
                {title}
              </h3>
            </div>
          </div>
          {content}
        </div>
      </body>
    </html>
  }

  /** Returns a page with the coflowemu css/js and a simple format. Used for scheduler UI. */
  def basicVarysPage(content: => Seq[Node], title: String): Seq[Node] = {
    <html>
      <head>
        <meta http-equiv="Content-type" content="text/html; charset=utf-8" />
        <link rel="stylesheet" href={prependBaseUri("/static/bootstrap.min.css")} type="text/css" />
        <link rel="stylesheet" href={prependBaseUri("/static/webui.css")}  type="text/css" />
        <script src={prependBaseUri("/static/sorttable.js")} ></script>
        <title>{title}</title>
      </head>
      <body>
        <div class="container-fluid">
          <div class="row-fluid">
            <div class="span12">
              <h3 style="vertical-align: middle; display: inline-block;">
                {title}
              </h3>
            </div>
          </div>
          {content}
        </div>
      </body>
    </html>
  }

  /** Returns an HTML table constructed by generating a row for each object in a sequence. */
  def listingTable[T](
      headers: Seq[String],
      makeRow: T => Seq[Node],
      rows: Seq[T],
      fixedWidth: Boolean = false): Seq[Node] = {

    val colWidth = 100.toDouble / headers.size
    val colWidthAttr = if (fixedWidth) colWidth + "%" else ""
    var tableClass = "table table-bordered table-striped table-condensed sortable"
    if (fixedWidth) {
      tableClass += " table-fixed"
    }

    <table class={tableClass}>
      <thead>{headers.map(h => <th width={colWidthAttr}>{h}</th>)}</thead>
      <tbody>
        {rows.map(r => makeRow(r))}
      </tbody>
    </table>
  }
}
