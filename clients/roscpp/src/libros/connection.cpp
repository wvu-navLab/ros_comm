/*
 * Software License Agreement (BSD License)
 *
 *  Copyright (c) 2008, Willow Garage, Inc.
 *  All rights reserved.
 *
 *  Redistribution and use in source and binary forms, with or without
 *  modification, are permitted provided that the following conditions
 *  are met:
 *
 *   * Redistributions of source code must retain the above copyright
 *     notice, this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above
 *     copyright notice, this list of conditions and the following
 *     disclaimer in the documentation and/or other materials provided
 *     with the distribution.
 *   * Neither the name of Willow Garage, Inc. nor the names of its
 *     contributors may be used to endorse or promote products derived
 *     from this software without specific prior written permission.
 *
 *  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 *  "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 *  LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS
 *  FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE
 *  COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
 *  INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 *  BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 *  LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
 *  CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 *  LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN
 *  ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 *  POSSIBILITY OF SUCH DAMAGE.
 */

#include "ros/connection.h"
#include "ros/transport/transport.h"
#include "ros/file_log.h"

#include <ros/assert.h>

#include <boost/shared_array.hpp>
#include <boost/bind.hpp>

#if AMISHARE_ROS == 1
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#endif

namespace ros
{

Connection::Connection()
: is_server_(false)
, dropped_(false)
, read_filled_(0)
, read_size_(0)
, reading_(false)
, has_read_callback_(0)
, write_sent_(0)
, write_size_(0)
, writing_(false)
, has_write_callback_(0)
, sending_header_error_(false)
{
#if AMISHARE_ROS == 1
  std::string filename = "/read_connection";
  read_connection_filename_ = AMISHARE_ROS_PATH + filename;
  filename = "/write_connection";
  write_connection_filename_ = AMISHARE_ROS_PATH + filename;
#endif
}

Connection::~Connection()
{
  ROS_DEBUG_NAMED("superdebug", "Connection destructing, dropped=%s", dropped_ ? "true" : "false");

  drop(Destructing);
}

void Connection::initialize(const TransportPtr& transport, bool is_server, const HeaderReceivedFunc& header_func)
{
  ROS_ASSERT(transport);

  transport_ = transport;
  header_func_ = header_func;
  is_server_ = is_server;

  transport_->setReadCallback(boost::bind(&Connection::onReadable, this, boost::placeholders::_1));
  transport_->setWriteCallback(boost::bind(&Connection::onWriteable, this, boost::placeholders::_1));
  transport_->setDisconnectCallback(boost::bind(&Connection::onDisconnect, this, boost::placeholders::_1));

  if (header_func)
  {
#if AMISHARE_ROS == 1
    read(4, read_connection_filename_, boost::bind(&Connection::onHeaderLengthRead, this, boost::placeholders::_1, boost::placeholders::_2, boost::placeholders::_3, boost::placeholders::_4));
#else
    read(4, boost::bind(&Connection::onHeaderLengthRead, this, boost::placeholders::_1, boost::placeholders::_2, boost::placeholders::_3, boost::placeholders::_4));
#endif
  }
}

boost::signals2::connection Connection::addDropListener(const DropFunc& slot)
{
  boost::recursive_mutex::scoped_lock lock(drop_mutex_);
  return drop_signal_.connect(slot);
}

void Connection::removeDropListener(const boost::signals2::connection& c)
{
  boost::recursive_mutex::scoped_lock lock(drop_mutex_);
  c.disconnect();
}

void Connection::onReadable(const TransportPtr& transport)
{
  (void)transport;
  ROS_ASSERT(transport == transport_);

#if AMISHARE_ROS == 1
  if (read_connection_filename_ != "")
  {
    readTransport(read_connection_filename_);
  }
  else
  {
  printf("*** readTransport before filename\n");
    readTransport();
  }
#else
  readTransport();
#endif
}

#if AMISHARE_ROS == 1
void Connection::readTransport(std::string name)
{
printf("connection readTransport with name %s\n", name.c_str());
  boost::recursive_mutex::scoped_try_lock lock(read_mutex_);

  if (!lock.owns_lock() || dropped_ || reading_)
  {
    return;
  }

  reading_ = true;

  while (!dropped_ && has_read_callback_)
  {
    ROS_ASSERT(read_buffer_);
    uint32_t to_read = read_size_ - read_filled_;
    if (to_read > 0)
    {
      connection_file_fd_ = open(read_connection_filename_.c_str(), O_RDONLY);
printf("tried to open file to read %s\n", read_connection_filename_.c_str());
      int32_t bytes_read = ::read(connection_file_fd_, read_buffer_.get() + read_filled_, to_read);
      if (bytes_read == -1) perror("read");
      close(connection_file_fd_);
printf("read %d bytes [%s]\n", bytes_read, read_buffer_.get());
      ROS_DEBUG_NAMED("superdebug", "Connection read %d bytes", bytes_read);
      if (dropped_)
      {
        return;
      }
      else if (bytes_read < 0)
      {
        // Bad read, throw away results and report error
        ReadFinishedFunc callback;
        callback = read_callback_;
        read_callback_.clear();
        read_buffer_.reset();
        uint32_t size = read_size_;
        read_size_ = 0;
        read_filled_ = 0;
        has_read_callback_ = 0;

        if (callback)
        {
          callback(shared_from_this(), read_buffer_, size, false);
        }

        break;
      }

      read_filled_ += bytes_read;
    }

    ROS_ASSERT((int32_t)read_size_ >= 0);
    ROS_ASSERT((int32_t)read_filled_ >= 0);
    ROS_ASSERT_MSG(read_filled_ <= read_size_, "read_filled_ = %d, read_size_ = %d", read_filled_, read_size_);

    if (read_filled_ == read_size_ && !dropped_)
    {
      ReadFinishedFunc callback;
      uint32_t size;
      boost::shared_array<uint8_t> buffer;

      ROS_ASSERT(has_read_callback_);

      // store off the read info in case another read() call is made from within the callback
      callback = read_callback_;
      size = read_size_;
      buffer = read_buffer_;
      read_callback_.clear();
      read_buffer_.reset();
      read_size_ = 0;
      read_filled_ = 0;
      has_read_callback_ = 0;

      ROS_DEBUG_NAMED("superdebug", "Calling read callback");
printf("read callback from readTransport for name %s\n", name.c_str());
      callback(shared_from_this(), buffer, size, true);
    }
    else
    {
      break;
    }
  }

  if (!has_read_callback_)
  {
    transport_->disableRead();
  }

  reading_ = false;
}
#endif
void Connection::readTransport()
{
printf("*** connection readTransport\n");
  boost::recursive_mutex::scoped_try_lock lock(read_mutex_);

  if (!lock.owns_lock() || dropped_ || reading_)
  {
    return;
  }

  reading_ = true;

  while (!dropped_ && has_read_callback_)
  {
    ROS_ASSERT(read_buffer_);
    uint32_t to_read = read_size_ - read_filled_;
    if (to_read > 0)
    {
      int32_t bytes_read = transport_->read(read_buffer_.get() + read_filled_, to_read);
      ROS_DEBUG_NAMED("superdebug", "Connection read %d bytes", bytes_read);
      if (dropped_)
      {
        return;
      }
      else if (bytes_read < 0)
      {
        // Bad read, throw away results and report error
        ReadFinishedFunc callback;
        callback = read_callback_;
        read_callback_.clear();
        read_buffer_.reset();
        uint32_t size = read_size_;
        read_size_ = 0;
        read_filled_ = 0;
        has_read_callback_ = 0;

        if (callback)
        {
          callback(shared_from_this(), read_buffer_, size, false);
        }

        break;
      }

      read_filled_ += bytes_read;
    }

    ROS_ASSERT((int32_t)read_size_ >= 0);
    ROS_ASSERT((int32_t)read_filled_ >= 0);
    ROS_ASSERT_MSG(read_filled_ <= read_size_, "read_filled_ = %d, read_size_ = %d", read_filled_, read_size_);

    if (read_filled_ == read_size_ && !dropped_)
    {
      ReadFinishedFunc callback;
      uint32_t size;
      boost::shared_array<uint8_t> buffer;

      ROS_ASSERT(has_read_callback_);

      // store off the read info in case another read() call is made from within the callback
      callback = read_callback_;
      size = read_size_;
      buffer = read_buffer_;
      read_callback_.clear();
      read_buffer_.reset();
      read_size_ = 0;
      read_filled_ = 0;
      has_read_callback_ = 0;

      ROS_DEBUG_NAMED("superdebug", "Calling read callback");
      callback(shared_from_this(), buffer, size, true);
    }
    else
    {
      break;
    }
  }

  if (!has_read_callback_)
  {
    transport_->disableRead();
  }

  reading_ = false;
}

#if AMISHARE_ROS == 1
void Connection::writeTransport(std::string name)
{
  boost::recursive_mutex::scoped_try_lock lock(write_mutex_);

  if (!lock.owns_lock() || dropped_ || writing_)
  {
    return;
  }

  writing_ = true;
  bool can_write_more = true;

  while (has_write_callback_ && can_write_more && !dropped_)
  {
    if (name != write_connection_filename_)
    {
      write_connection_filename_ = name;
    }
    uint32_t to_write = write_size_ - write_sent_;
    ROS_DEBUG_NAMED("superdebug", "Connection writing %d bytes", to_write);
    mkfifo(write_connection_filename_.c_str(), 0666);
    connection_file_fd_ = open(write_connection_filename_.c_str(), O_WRONLY | O_CREAT);
 printf("open file for writing %s\n", write_connection_filename_.c_str());
 printf("write_sent_ %d, to_write %d\n", write_sent_, to_write);
 printf("write buffer [%s]\n", write_buffer_.get());
    int ret;
    ret = ::write(connection_file_fd_, write_buffer_.get() + write_sent_, to_write);
    if (ret == -1) perror("write");
    //ret = ::write(connection_file_fd_, "\n", sizeof(char));
    //if (ret == -1) perror("write");
    close(connection_file_fd_);
    int32_t bytes_sent = to_write;
    ROS_DEBUG_NAMED("superdebug", "Connection wrote %d bytes", bytes_sent);

    if (bytes_sent < 0)
    {
      writing_ = false;
      return;
    }

    write_sent_ += bytes_sent;

    if (bytes_sent < (int)write_size_ - (int)write_sent_)
    {
      can_write_more = false;
    }

    if (write_sent_ == write_size_ && !dropped_)
    {
      WriteFinishedFunc callback;

      {
        boost::mutex::scoped_lock lock(write_callback_mutex_);
        ROS_ASSERT(has_write_callback_);
        // Store off a copy of the callback in case another write() call happens in it
        callback = write_callback_;
        write_callback_ = WriteFinishedFunc();
        write_buffer_ = boost::shared_array<uint8_t>();
        write_sent_ = 0;
        write_size_ = 0;
        has_write_callback_ = 0;
      }

      ROS_DEBUG_NAMED("superdebug", "Calling write callback");
      callback(shared_from_this());
    }
  }

  {
    boost::mutex::scoped_lock lock(write_callback_mutex_);
    if (!has_write_callback_)
    {
      transport_->disableWrite();
    }
  }

  writing_ = false;
}
#endif
void Connection::writeTransport()
{
  boost::recursive_mutex::scoped_try_lock lock(write_mutex_);

  if (!lock.owns_lock() || dropped_ || writing_)
  {
    return;
  }

  writing_ = true;
  bool can_write_more = true;

  while (has_write_callback_ && can_write_more && !dropped_)
  {
    uint32_t to_write = write_size_ - write_sent_;
    ROS_DEBUG_NAMED("superdebug", "Connection writing %d bytes", to_write);
    int32_t bytes_sent = transport_->write(write_buffer_.get() + write_sent_, to_write);
    ROS_DEBUG_NAMED("superdebug", "Connection wrote %d bytes", bytes_sent);

    if (bytes_sent < 0)
    {
      writing_ = false;
      return;
    }

    write_sent_ += bytes_sent;

    if (bytes_sent < (int)write_size_ - (int)write_sent_)
    {
      can_write_more = false;
    }

    if (write_sent_ == write_size_ && !dropped_)
    {
      WriteFinishedFunc callback;

      {
        boost::mutex::scoped_lock lock(write_callback_mutex_);
        ROS_ASSERT(has_write_callback_);
        // Store off a copy of the callback in case another write() call happens in it
        callback = write_callback_;
        write_callback_ = WriteFinishedFunc();
        write_buffer_ = boost::shared_array<uint8_t>();
        write_sent_ = 0;
        write_size_ = 0;
        has_write_callback_ = 0;
      }

      ROS_DEBUG_NAMED("superdebug", "Calling write callback");
      callback(shared_from_this());
    }
  }

  {
    boost::mutex::scoped_lock lock(write_callback_mutex_);
    if (!has_write_callback_)
    {
      transport_->disableWrite();
    }
  }

  writing_ = false;
}

void Connection::onWriteable(const TransportPtr& transport)
{
  (void)transport;
  ROS_ASSERT(transport == transport_);

  writeTransport();
}

#if AMISHARE_ROS == 1
void Connection::read(uint32_t size, std::string name, const ReadFinishedFunc& callback)
{
printf("connection read with name %s\n", name.c_str());
  if (read_connection_filename_ != name)
  {
    read_connection_filename_ = name;
    printf("set filename %s\n", read_connection_filename_.c_str());
    size_t start, end;
    start = name.find_last_of("/");
    end = name.find_last_of(".");
    service_name_ = name.substr(start, end-start);
    printf("name positions %d %d\n", start, end);
    printf("***** service name %s\n", service_name_.c_str());
    
  //if the path includes a directory that doesn't exist, make it before open
    /*
    size_t position = 1;
    size_t found = read_connection_filename_.find_first_of("/", position);
    while (found != std::string::npos)
    {
      position = found+1;
      std::string directory = read_connection_filename_.substr(0, found);
      std::string openpath = directory;
      mkdir(openpath.c_str(), 0775);
      printf("directory created at %s\n", openpath.c_str());
      found = read_connection_filename_.find_first_of("/", position);
    }
    */
  }

  if (dropped_ || sending_header_error_)
  {
    return;
  }

  {
    boost::recursive_mutex::scoped_lock lock(read_mutex_);

    ROS_ASSERT(!read_callback_);

    read_callback_ = callback;
    read_buffer_ = boost::shared_array<uint8_t>(new uint8_t[size]);
    read_size_ = size;
    read_filled_ = 0;
    has_read_callback_ = 1;
  }

  transport_->enableRead();

  // read immediately if possible
  readTransport(name);
}
#endif
void Connection::read(uint32_t size, const ReadFinishedFunc& callback)
{
  if (dropped_ || sending_header_error_)
  {
    return;
  }

  {
    boost::recursive_mutex::scoped_lock lock(read_mutex_);

    ROS_ASSERT(!read_callback_);

    read_callback_ = callback;
    read_buffer_ = boost::shared_array<uint8_t>(new uint8_t[size]);
    read_size_ = size;
    read_filled_ = 0;
    has_read_callback_ = 1;
  }

  transport_->enableRead();

  // read immediately if possible
 printf("*** readTransport from read with no filename\n");
  readTransport();
}

#if AMISHARE_ROS == 1
void Connection::write(const boost::shared_array<uint8_t>& buffer, uint32_t size, std::string name, const WriteFinishedFunc& callback, bool immediate)
{
printf("connection write with name %s\n", name.c_str());
  if (write_connection_filename_ != name)
  {
    write_connection_filename_ = name;
    printf("set filename %s\n", write_connection_filename_.c_str());
    
  //if the path includes a directory that doesn't exist, make it before open
    /*
    size_t position = 1;
    size_t found = write_connection_filename_.find_first_of("/", position);
    while (found != std::string::npos)
    {
      position = found+1;
      std::string directory = write_connection_filename_.substr(0, found);
      std::string openpath = directory;
      mkdir(openpath.c_str(), 0775);
      printf("directory created at %s\n", openpath.c_str());
      found = write_connection_filename_.find_first_of("/", position);
    }
    */
  }
  if (dropped_ || sending_header_error_)
  {
    return;
  }

  {
    boost::mutex::scoped_lock lock(write_callback_mutex_);

    ROS_ASSERT(!write_callback_);

    write_callback_ = callback;
    write_buffer_ = buffer;
    write_size_ = size;
    write_sent_ = 0;
    has_write_callback_ = 1;
  }

  transport_->enableWrite();

  if (immediate)
  {
    // write immediately if possible
    writeTransport(name);
  }
}
#endif
void Connection::write(const boost::shared_array<uint8_t>& buffer, uint32_t size, const WriteFinishedFunc& callback, bool immediate)
{
  if (dropped_ || sending_header_error_)
  {
    return;
  }

  {
    boost::mutex::scoped_lock lock(write_callback_mutex_);

    ROS_ASSERT(!write_callback_);

    write_callback_ = callback;
    write_buffer_ = buffer;
    write_size_ = size;
    write_sent_ = 0;
    has_write_callback_ = 1;
  }

  transport_->enableWrite();

  if (immediate)
  {
    // write immediately if possible
    writeTransport();
  }
}

void Connection::onDisconnect(const TransportPtr& transport)
{
  (void)transport;
  ROS_ASSERT(transport == transport_);

  drop(TransportDisconnect);
}

void Connection::drop(DropReason reason)
{
  ROSCPP_LOG_DEBUG("Connection::drop(%u)", reason);
  bool did_drop = false;
  {
    boost::recursive_mutex::scoped_lock lock(drop_mutex_);
    if (!dropped_)
    {
      dropped_ = true;
      did_drop = true;
    }
  }

  if (did_drop)
  {
    transport_->close();
    {
      boost::recursive_mutex::scoped_lock lock(drop_mutex_);
      drop_signal_(shared_from_this(), reason);
    }
  }
}

bool Connection::isDropped()
{
  boost::recursive_mutex::scoped_lock lock(drop_mutex_);
  return dropped_;
}

void Connection::writeHeader(const M_string& key_vals, const WriteFinishedFunc& finished_callback)
{
  ROS_ASSERT(!header_written_callback_);
  header_written_callback_ = finished_callback;

  if (!transport_->requiresHeader())
  {
    onHeaderWritten(shared_from_this());
    return;
  }

  boost::shared_array<uint8_t> buffer;
  uint32_t len;
  Header::write(key_vals, buffer, len);

  uint32_t msg_len = len + 4;
  boost::shared_array<uint8_t> full_msg(new uint8_t[msg_len]);
  memcpy(full_msg.get() + 4, buffer.get(), len);
  *((uint32_t*)full_msg.get()) = len;

#if AMISHARE_ROS == 1
  write(full_msg, msg_len, write_connection_filename_, boost::bind(&Connection::onHeaderWritten, this, boost::placeholders::_1), false);
#else
  write(full_msg, msg_len, boost::bind(&Connection::onHeaderWritten, this, boost::placeholders::_1), false);
#endif
}

void Connection::sendHeaderError(const std::string& error_msg)
{
  M_string m;
  m["error"] = error_msg;

  writeHeader(m, boost::bind(&Connection::onErrorHeaderWritten, this, boost::placeholders::_1));
  sending_header_error_ = true;
}

void Connection::onHeaderLengthRead(const ConnectionPtr& conn, const boost::shared_array<uint8_t>& buffer, uint32_t size, bool success)
{
  (void)size;
  ROS_ASSERT(conn.get() == this);
  ROS_ASSERT(size == 4);

  if (!success)
    return;

  uint32_t len = *((uint32_t*)buffer.get());

  if (len > 1000000000)
  {
    ROS_ERROR("a header of over a gigabyte was " \
                "predicted in tcpros. that seems highly " \
                "unlikely, so I'll assume protocol " \
                "synchronization is lost.");
    conn->drop(HeaderError);
  }

#if AMISHARE_ROS == 1
printf("new read from file %s\n", read_connection_filename_.c_str());
  read(len, read_connection_filename_, boost::bind(&Connection::onHeaderRead, this, boost::placeholders::_1, boost::placeholders::_2, boost::placeholders::_3, boost::placeholders::_4));
#else
  read(len, boost::bind(&Connection::onHeaderRead, this, boost::placeholders::_1, boost::placeholders::_2, boost::placeholders::_3, boost::placeholders::_4));
#endif
}

void Connection::onHeaderRead(const ConnectionPtr& conn, const boost::shared_array<uint8_t>& buffer, uint32_t size, bool success)
{
  ROS_ASSERT(conn.get() == this);

  if (!success)
    return;

  std::string error_msg;
  if (!header_.parse(buffer, size, error_msg))
  {
    drop(HeaderError);
  }
  else
  {
    std::string error_val;
    if (header_.getValue("error", error_val))
    {
      ROSCPP_LOG_DEBUG("Received error message in header for connection to [%s]: [%s]", transport_->getTransportInfo().c_str(), error_val.c_str());
      drop(HeaderError);
    }
    else
    {
      ROS_ASSERT(header_func_);

      transport_->parseHeader(header_);

      header_func_(conn, header_);
    }
  }

}

void Connection::onHeaderWritten(const ConnectionPtr& conn)
{
  ROS_ASSERT(conn.get() == this);
  ROS_ASSERT(header_written_callback_);

  header_written_callback_(conn);
  header_written_callback_ = WriteFinishedFunc();
}

void Connection::onErrorHeaderWritten(const ConnectionPtr& conn)
{
  (void)conn;
  drop(HeaderError);
}

void Connection::setHeaderReceivedCallback(const HeaderReceivedFunc& func)
{
  header_func_ = func;

  if (transport_->requiresHeader())
#if AMISHARE_ROS == 1
    read(4, read_connection_filename_, boost::bind(&Connection::onHeaderLengthRead, this, boost::placeholders::_1, boost::placeholders::_2, boost::placeholders::_3, boost::placeholders::_4));
#else
    read(4, boost::bind(&Connection::onHeaderLengthRead, this, boost::placeholders::_1, boost::placeholders::_2, boost::placeholders::_3, boost::placeholders::_4));
#endif
}

std::string Connection::getCallerId()
{
  std::string callerid;
  if (header_.getValue("callerid", callerid))
  {
    return callerid;
  }

  return std::string("unknown");
}

std::string Connection::getRemoteString()
{
  std::stringstream ss;
  ss << "callerid=[" << getCallerId() << "] address=[" << transport_->getTransportInfo() << "]";
  return ss.str();
}

}
