module rediv

import (
  net
  sync
)

pub struct Conn {
  sock net.Socket

mut:
  mutex sync.Mutex
}

pub fn dial(address string, port int) ?Conn {
  sock := net.dial(address, port) or {
    return error(err)
  }
  mutex := sync.new_mutex()
  c := Conn {
    sock: sock
    mutex: mutex
  }
  return c
}

fn (c Conn) read_line() string {
  mut res := ''
  for {
    res += c.sock.read_line()
    if res[res.len-2..res.len] == '\r\n' {
      return res[0..res.len-2]
    }
  }
  return res
}

pub fn (c mut Conn) do(args []string) ?[]string {
  mut req := '*'
  req += args.len.str()
  req += '\r\n'
  for _, arg in args {
    req += '$'
    req += arg.len.str()
    req += '\r\n'
    req += arg
    req += '\r\n'
  }

  c.mutex.lock()
  defer { c.mutex.unlock() }
  c.sock.send(req.str, req.len) or {
    return error(err)
  }

  resp := c.read_line()
  match resp[0] {
    `+` { return [resp[1..resp.len]] }
    `-` { return error(resp[1..resp.len]) }
    `:` { return [resp[1..resp.len]] }
    `$` { 
      len := resp[1..resp.len].int()
      if len <= 0 {
        return []string
      }
      return [c.read_line()]
    }
    `*` {
      mut res := []string
      len := resp[1..resp.len].int()
      for i := 0; i < len; i++ {
        r := c.read_line()
	if r[0..1] != '$' {
          return error('protocol error')
	}
	l := r[1..resp.len].int()
	if l <= 0 {
          continue
	}
        res << c.read_line()
      }
      return res
    }
    else {
      return error('protocol error')
    }
  }
}
